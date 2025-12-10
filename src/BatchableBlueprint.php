<?php

namespace Daursu\ZeroDowntimeMigration;

use Illuminate\Database\Schema\Blueprint;

/**
 * A variant of `Blueprint` that allows for connection types to define a `statements`
 * function to process an array of SQL query strings at once.
 */
class BatchableBlueprint extends Blueprint
{
    /**
     * @see Blueprint::build
     */
    public function build(Connection $connection, Grammar $grammar)
    {
        $statements = $this->toSql($connection, $grammar);

        // Allow connections to run multiple statements at once if they support it.
        // For example, pt-online-schema-change does for running multiple operations.
        // on the cloned / "_new" table.
        if (!empty($statements) && method_exists($connection, 'statements')) {
            return $connection->statements($statements);
        }

        // Default / "parent" logic - but re-using `$statements`.
        foreach ($statements as $statement) {
            $connection->statement($statement);
        }
    }
}
