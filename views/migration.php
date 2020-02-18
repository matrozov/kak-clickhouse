<?php

/* @var $className string the new migration class name */
echo "<?php\n";
if (!empty($namespace)) {
    echo "\nnamespace {$namespace};\n";
}

?>

class <?= $className ?> extends \kak\clickhouse\Migration
{
    public function up()
    {

    }

    public function down()
    {
        echo "<?= $className ?> cannot be reverted.\n";

        return false;
    }
}