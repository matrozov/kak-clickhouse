<?php

namespace kak\clickhouse;

use yii\base\Component;
use yii\base\InvalidConfigException;
use yii\db\ColumnSchemaBuilder;
use yii\db\Exception;
use yii\db\MigrationInterface;
use yii\di\Instance;
use yii\helpers\StringHelper;

/**
 * Class Migration
 * @package kak\clickhouse
 */
abstract class Migration extends Component implements MigrationInterface
{
    /**
     * @var Connection|array|string
     */
    public $db = 'clickhouse';

    /**
     * {@inheritDoc}
     * @throws InvalidConfigException
     */
    public function init()
    {
        parent::init();

        $this->db = Instance::ensure($this->db, Connection::class);
        $this->db->getSchema()->refresh();
    }

    /**
     * {@inheritdoc}
     * @since 2.0.6
     */
    protected function getDb()
    {
        return $this->db;
    }

    public function up()
    {
    }

    public function down()
    {
    }

    /**
     * @param $sql
     * @param array $params
     * @throws Exception
     */
    public function execute($sql, $params = [])
    {
        $sqlOutput = $sql;

        $time = $this->beginCommand("execute SQL: $sqlOutput");
        $this->db->createCommand($sql)->bindValues($params)->execute();
        $this->endCommand($time);
    }

    /**
     * @param string $table the table that new rows will be inserted into.
     * @param array $columns the column data (name => value) to be inserted into the table.
     * @throws Exception
     */
    public function insert($table, $columns)
    {
        $time = $this->beginCommand("insert into $table");

        $this->db->createCommand()->insert($table, $columns)->execute();

        $this->endCommand($time);
    }

    /**
     * @param string $table the name of the table to be created. The name will be properly quoted by the method.
     * @param array $columns the columns (name => definition) in the new table.
     * @param string $options additional SQL fragment that will be appended to the generated SQL.
     * @throws Exception
     */
    public function createTable($table, $columns, $options = null)
    {
        $time = $this->beginCommand("create table $table");

        $this->db->createCommand()->createTable($table, $columns, $options)->execute();

        foreach ($columns as $column => $type) {
            if ($type instanceof ColumnSchemaBuilder && $type->comment !== null) {
                $this->db->createCommand()->addCommentOnColumn($table, $column, $type->comment)->execute();
            }
        }

        $this->endCommand($time);
    }

    /**
     * @param $table
     * @throws Exception
     */
    public function dropTable($table)
    {
        $time = $this->beginCommand("drop table $table");
        $this->db->createCommand()->dropTable($table)->execute();
        $this->endCommand($time);
    }

    /**
     * Prepares for a command to be executed, and outputs to the console.
     *
     * @param string $description the description for the command, to be output to the console.
     * @return float the time before the command is executed, for the time elapsed to be calculated.
     * @since 2.0.13
     */
    protected function beginCommand($description)
    {
        echo "    > $description ...";

        return microtime(true);
    }

    /**
     * Finalizes after the command has been executed, and outputs to the console the time elapsed.
     *
     * @param float $time the time before the command was executed.
     * @since 2.0.13
     */
    protected function endCommand($time)
    {
        echo ' done (time: ' . sprintf('%.3f', microtime(true) - $time) . "s)\n";
    }
}