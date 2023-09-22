import boto3
import datetime
import json

def create_rds_snapshot(event, context):
    rds = boto3.client('rds')
    db_instance_identifier = 'database-Name'
    current_date = datetime.datetime.now()
    week_number = current_date.isocalendar()[1]  # Get the ISO week number
    month_number = current_date.month
    year_number = current_date.year

    def create_snapshot(snapshot_identifier):
        rds.create_db_snapshot(
            DBSnapshotIdentifier=snapshot_identifier,
            DBInstanceIdentifier=db_instance_identifier
        )

    def delete_snapshots(snapshots, num_to_keep):
        snapshots_to_delete = sorted(snapshots, key=lambda s: s['SnapshotCreateTime'])
        for snapshot in snapshots_to_delete:
            rds.delete_db_snapshot(
                DBSnapshotIdentifier=snapshot['DBSnapshotIdentifier']
            )

    if 'CreateSnapshot' in event:
        snapshot_type = event['CreateSnapshot']
        if snapshot_type == 'WeeklySnapshot':
            # Create weekly snapshot
            existing_weekly_snapshots = [snapshot for snapshot in rds.describe_db_snapshots(
                DBInstanceIdentifier=db_instance_identifier,
            )['DBSnapshots'] if snapshot['DBSnapshotIdentifier'].startswith(f'weekly-{db_instance_identifier}')]

            previous_snapshot = max(existing_weekly_snapshots, key=lambda s: s['SnapshotCreateTime']) if existing_weekly_snapshots else None
            previous_month_number = previous_snapshot['SnapshotCreateTime'].month if previous_snapshot else None

            if current_date.weekday() == 3 and (previous_month_number is None or previous_month_number != month_number):
                new_weekly_serial_number = max([int(snapshot['DBSnapshotIdentifier'].split('-')[-1]) for snapshot in existing_weekly_snapshots], default=0) + 1
                create_snapshot(f'weekly-{db_instance_identifier}-{year_number}-{month_number:02d}-{new_weekly_serial_number}')
                delete_snapshots(existing_weekly_snapshots, 1)

        if snapshot_type == 'MonthlySnapshot':
            # Create monthly snapshot if the month has changed
            previous_month = current_date.month - 1 if current_date.month > 1 else 12
            if current_date.day == 1 and previous_month != month_number:
                existing_monthly_snapshots = [snapshot for snapshot in rds.describe_db_snapshots(
                    DBInstanceIdentifier=db_instance_identifier,
                )['DBSnapshots'] if snapshot['DBSnapshotIdentifier'].startswith(f'Monthly-{db_instance_identifier}')]

                new_monthly_serial_number = max([int(snapshot['DBSnapshotIdentifier'].split('-')[-1]) for snapshot in existing_monthly_snapshots], default=0) + 1
                create_snapshot(f'Monthly-{db_instance_identifier}-{year_number}-{month_number:02d}-{new_monthly_serial_number}')
                delete_snapshots(existing_monthly_snapshots, 11)

        if snapshot_type == 'YearlySnapshot':
            # Create yearly snapshot
            if current_date.month == 1 and current_date.day == 1:  # First day of the year
                existing_yearly_snapshots = [snapshot for snapshot in rds.describe_db_snapshots(
                    DBInstanceIdentifier=db_instance_identifier,
                )['DBSnapshots'] if snapshot['DBSnapshotIdentifier'].startswith(f'Yearly-{db_instance_identifier}')]

                
