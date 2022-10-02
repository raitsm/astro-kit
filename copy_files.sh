#!/bin/bash
cp ../via-piudzpa-helper-files/asteroids/config.ini.template .
cp ../via-piudzpa-helper-files/asteroids/log_migrate_db.yaml.dev .
cp ../via-piudzpa-helper-files/asteroids/log_worker.yaml.dev .
cp ../via-piudzpa-helper-files/asteroids/migrate_db.py .
cp ../via-piudzpa-helper-files/asteroids/naked.py .
cp ../via-piudzpa-helper-files/asteroids/test_config.py .
cp ../via-piudzpa-helper-files/asteroids/test_worker.py .
cp ../via-piudzpa-helper-files/asteroids/worker_2_db.py .
cp -r ../via-piudzpa-helper-files/asteroids/migrations .
mkdir -p log
touch log/logs_go_here

