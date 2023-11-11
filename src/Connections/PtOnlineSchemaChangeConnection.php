<?php

namespace Daursu\ZeroDowntimeMigration\Connections;

use Illuminate\Support\Collection;
use Symfony\Component\Process\Process;

class PtOnlineSchemaChangeConnection extends BaseConnection
{
    /**
     * Executes the SQL statement through pt-online-schema-change.
     *
     * @param  string $query
     * @param  array  $bindings
     * @return bool|int
     */
    public function statement($query, $bindings = [])
    {
        return $this->runQueries([$query]);
    }

    /**
     * A custom connection method called by our custom schema builder to help batch
     * operations on the cloned table.
     *
     * @see \Daursu\ZeroDowntimeMigration\BatchableBlueprint
     *
     * @param string $query
     * @param array $bindings
     * @return bool|int
     */
    public function statements($queries, $bindings = [])
    {
        return $this->runQueries($queries);
    }

    /**
     * @param  string $table
     * @return string
     */
    protected function getAuthString(string $table): string
    {
        return sprintf(
            'h=%s,P=%s,D=%s,u=%s,p=%s,t=%s',
            $this->getConfig('host'),
            $this->getConfig('port'),
            $this->getConfig('database'),
            $this->getConfig('username'),
            $this->getConfig('password'),
            $table
        );
    }

    /**
     * Hide the username/pw from console output.
     *
     * @param array $command
     * @return string
     */
    protected function maskSensitiveInformation(array $command): string
    {
        return collect($command)->map(function ($config) {
            $config = preg_replace('/('.preg_quote($this->getConfig('password'), '/').'),/', '*****,', $config);

            return preg_replace('/('.preg_quote($this->getConfig('username'), '/').'),/', '*****,', $config);
        })->implode(' ');
    }

    /**
     * @param string[] $queries
     * @return bool|int
     */
    protected function runQueries($queries)
    {
        $table = $this->extractTableFromQuery($queries[0]);
        $cleanQueries = collect($queries)->map(function  ($query) {
            return $this->cleanQuery($query);
        });
        $transformedQueries = $this->getTransformedQueries($table, $cleanQueries);

        return $this->runProcess(
            $this->makeCommand($table, $transformedQueries, $this->isPretending())
        );
    }

    /**
     * @param Collection<string> $queries
     * @return Collection<string>
     */
    private function getTransformedQueries(string $table, Collection $queries): Collection
    {
        $baseNameToNewName = $this->getNewForeignKeyNameMap($table, $queries);

        return $queries->map(function ($query) use ($baseNameToNewName) {
            $key = str($query)->match("/drop foreign key `(.*?)`/i");
            if (!$key) {
                return $query;
            }

            $baseName = ltrim($key, '_');
            $newName = $baseNameToNewName->get($baseName);

            return $newName ? str($query)->replace("`$key`", "`$newName`")->value() : $query;
        });
    }

    /**
     * @param Collection<string> $queries
     * @return Collection<string>
     */
    private function getNewForeignKeyNameMap(string $table, Collection $queries): Collection
    {
        $process = new Process($this->makeCommand($table, $queries, true));
        $process->run();
        $process->stop();

        $newForeignConstraintNames = str($process->getOutput())
            ->matchAll("/constraint `(.*?)`/i");

        return $newForeignConstraintNames->keyBy(function ($name) {
            return ltrim($name, '_');
        });
    }

    private function makeCommand(string $table, Collection $queries, bool $dryRun = false): array
    {
        // array_filter to strip empty lines from `getAdditionalParameters`
        return array_filter(array_merge(
            ['pt-online-schema-change', $dryRun ? '--dry-run' : '--execute'],
            $this->getAdditionalParameters(),
            ['--alter', $queries->join(', '), $this->getAuthString($table)]
        ));
    }
}
