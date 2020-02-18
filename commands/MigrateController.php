<?php

namespace kak\clickhouse\commands;

use kak\clickhouse\Connection;
use kak\clickhouse\Migration;
use kak\clickhouse\Query;
use Yii;
use yii\base\Action;
use yii\base\InvalidConfigException;
use yii\console\controllers\BaseMigrateController;
use yii\db\Exception;
use yii\di\Instance;
use yii\helpers\ArrayHelper;
use yii\helpers\Console;

/**
 * Class MigrateController
 * @package kak\clickhouse\commands
 */
class MigrateController extends BaseMigrateController
{
    public $migrationTable = 'migration';

    public $templateFile = '@kak/clickhouse/views/migration.php';

    /** @var Connection|array|string  */
    public $db = 'clickhouse';

    /**
     * {@inheritdoc}
     */
    public function options($actionID)
    {
        return array_merge(
            parent::options($actionID),
            ['migrationTable', 'db']
        );
    }

    /**
     * @param Action $action
     * @return bool
     * @throws InvalidConfigException
     */
    public function beforeAction($action)
    {
        if (parent::beforeAction($action)) {
            $this->db = Instance::ensure($this->db, Connection::className());
            return true;
        }

        return false;
    }

    /**
     * Creates a new migration instance.
     * @param string $class the migration class name
     * @return Migration the migration instance
     * @throws InvalidConfigException
     */
    protected function createMigration($class)
    {
        $this->includeMigrationFile($class);

        return Yii::createObject([
            'class' => $class,
            'db' => $this->db,
            'compact' => $this->compact,
        ]);
    }

    /**
     * {@inheritdoc}
     * @throws Exception
     */
    protected function getMigrationHistory($limit)
    {
        if ($this->db->schema->getTableSchema($this->migrationTable, true) === null) {
            $this->createMigrationHistoryTable();
        }
        $query = (new Query())
            ->select(['version', 'apply_time'])
            ->from($this->migrationTable)
            ->orderBy(['apply_time' => SORT_DESC, 'version' => SORT_DESC]);

        if (empty($this->migrationNamespaces)) {
            $query->limit($limit);
            $rows = $query->all($this->db);
            $history = ArrayHelper::map($rows, 'version', 'apply_time');
            unset($history[self::BASE_MIGRATION]);
            return $history;
        }

        $rows = $query->all($this->db);

        $history = [];
        foreach ($rows as $key => $row) {
            if ($row['version'] === self::BASE_MIGRATION) {
                continue;
            }
            if (preg_match('/m?(\d{6}_?\d{6})(\D.*)?$/is', $row['version'], $matches)) {
                $time = str_replace('_', '', $matches[1]);
                $row['canonicalVersion'] = $time;
            } else {
                $row['canonicalVersion'] = $row['version'];
            }
            $row['apply_time'] = (int) $row['apply_time'];
            $history[] = $row;
        }

        usort($history, function ($a, $b) {
            if ($a['apply_time'] === $b['apply_time']) {
                if (($compareResult = strcasecmp($b['canonicalVersion'], $a['canonicalVersion'])) !== 0) {
                    return $compareResult;
                }

                return strcasecmp($b['version'], $a['version']);
            }

            return ($a['apply_time'] > $b['apply_time']) ? -1 : +1;
        });

        $history = array_slice($history, 0, $limit);

        $history = ArrayHelper::map($history, 'version', 'apply_time');

        return $history;
    }

    /**
     * Creates the migration history table.
     * @throws Exception
     */
    protected function createMigrationHistoryTable()
    {
        $tableName = $this->db->schema->getRawTableName($this->migrationTable);
        $this->stdout("Creating migration history table \"$tableName\"...", Console::FG_YELLOW);

        $this->db->createCommand()->createTable($this->migrationTable, [
            'version' => 'String',
            'apply_time' => 'DateTime',
        ], 'ENGINE = MergeTree() PRIMARY KEY (version) ORDER BY (version)')->execute();

        $this->addMigrationHistory(self::BASE_MIGRATION);

        $this->stdout("Done.\n", Console::FG_GREEN);
    }

    /**
     * {@inheritdoc}
     * @throws Exception
     */
    protected function addMigrationHistory($version)
    {
        $command = $this->db->createCommand();
        $command->insert($this->migrationTable, [
            'version' => $version,
            'apply_time' => time(),
        ])->execute();
    }

    /**
     * {@inheritdoc}
     * @throws Exception
     */
    protected function removeMigrationHistory($version)
    {
        $this->db->createCommand('ALTER TABLE ' . $this->migrationTable . ' DELETE WHERE version = :version', [
            ':version' => $version,
        ])->execute();
    }

}