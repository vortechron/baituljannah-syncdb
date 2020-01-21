<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Contracts\Logging\Log;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\Storage;

class SyncDB extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'sync-db {--tables=}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Synchronize Database';

    protected $storage = 'sync-db.json';

    protected $liveDatabaseConnection = 'mysql';
    protected $syncDatabaseConnection = 'report';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $storage = [];
        $tables = collect(explode(',', $this->option('tables')));

        // File storage checker
        if (Storage::exists($this->storage)) {
            $storage = (array) json_decode(Storage::get($this->storage));
        } else {
            Storage::put($this->storage, json_encode($storage));
        }

        // Connect to production database
        $liveDB = DB::connection($this->liveDatabaseConnection);

        // Begin synching
        $tables->each(function ($table) use ($liveDB, &$storage) {
            if (! Schema::hasTable($table)) {
                $this->error('Table '. $table .' not exists!');
                return;
            }

            $offsetStorageKey = $table . '_offset';
            $offset = isset($storage[$offsetStorageKey]) ? $storage[$offsetStorageKey] : 0;
            $rowCounter = 0;

            $data = $liveDB->table($table)->offset($offset)->limit(PHP_INT_MAX)->get();
            foreach($data as $record){
                // Save data to staging database - default db connection
                DB::connection($this->syncDatabaseConnection)->table($table)->insert((array) $record);
            }

            $rowCounter += count($data);
            $this->line('Synching for table: '. $table);

            // Increment offset
            $storage[$offsetStorageKey] = $offset + $rowCounter;
        });

        // Update storage with latest offset
        Storage::put($this->storage, json_encode($storage));

        $this->info('Synching completed!');
    }
}
