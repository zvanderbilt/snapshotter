import boto
import boto.ec2
import sys
import time
import logging
import logging.config

logger = logging.getLogger('SnapshotPruneAndLog')
logger.setLevel(logging.ERROR)
SECONDS_PER_DAY=86400

def ec2backup(instanceRegion='us-east-1'):
	errorCount = 0
    try:
        # Connect to region
        ec2 = boto.ec2.connect_to_region(instanceRegion)

        # Collect local instance meta-data
        Metadata = boto.utils.get_instance_metadata()
        InstanceID = Metadata['instance-id']

        # Enumerate volumes attached to local instance
        Volumes = ec2.get_all_volumes(
                    filters={'attachment.instance-id': InstanceID})

        # Enumerate tags of local instance
        Tags = ec2.get_all_tags(filters={'resource-id': InstanceID})
        mytagdict = {}

        # Convert list to dictionary (for later keyed access)
        for tag in Tags:
            #print "Tag key="+tag.name+", value="+tag.value
            mytagdict[tag.name] = tag.value

        # Assign tag values to instance variables
        try:
            InstanceName = mytagdict['Name']
            RetentionCopies = int(mytagdict['Retention'])
            Environment = mytagdict['Environment']
            Project = mytagdict['Project']
            ServerBillTo = mytagdict['Server Bill To']
        except KeyError as e:
            print("ERROR: Instance %s does not have tag: %s" % (InstanceID, e))
            logger.error("ERROR: Instance %s does not have tag: %s" % (InstanceID, e))
            return 1

        # Iterate through list of volumes
        for volume in Volumes:
            # Enumerate snapshots for each volume
            Snapshots = ec2.get_all_snapshots(filters={"volume-id": volume})

            print "%s: Deleting stale snapshots. %s automated snapshots currently exist." % (volume, len(Snapshots))
			logger.error("%s: Deleting stale snapshots. %s automated snapshots currently exist." % (volume, len(Snapshots))
            DelStartTime = time.time()

            # Delete stale snapshots, log and ignore failures
            for snapshot in Snapshots:
                if DelStartTime >= snapshot.tags['DateToDelete']:
                    try:
                        ec2.delete_snapshot(snapshot.id)
                    except boto.exception.BotoServerError as e:
                        logger.error("ERROR: Snapshot %s deletion failed! %s" % (snapshot.id, e.error_message))

            print "%s: Creating new snapshot. %s automated snapshots currently exist." % (volume, len(Snapshots))

            # Create description string for new snapshot
            snapshotDescription = "Automated,LocalBackup,"
            snapshotDescription += volume.id
            SnapStartTime = time.time()
            DelTime = SnapStartTime
            DelTime += (RetentionCopies*SECONDS_PER_DAY)

            # Create new local snapshot
            try:
                Snapshot = ec2.create_snapshot(volume.id, snapshotDescription)
                Snapshot.add_tag('Name', value=InstanceName)
                Snapshot.add_tag('DateToDelete', value=DelTime)
                Snapshot.add_tag('Environment', value=Environment)
                Snapshot.add_tag('Project', value=Project)
                Snapshot.add_tag('Server Bill To', value=SeverBillTo)

            except boto.exception.BotoServerError as e:
                print("ERROR: Snapshot of %s failed! %s" % (volume.id, e.error_message))
                logger.error("ERROR: Snapshot of %s failed! %s" % (volume.id, e.error_message))

            Duration = time.time() - SnapStartTime
            print "%s: Completed new snapshot. Elapsed time in seconds: %d" % (volume, Duration)

            return 0

    except boto.exception.BotoServerError as e:
        print("ERROR: %s" % (e.error_message))
        logger.error("ERROR: %s" % (e.error_message))
        return 1


def main(argv=None):

    if argv is None or len(argv) < 2:
        print "Usage: SnapshotAndPrune <region>"
        return 2
    else:
        return ec2backup(instanceRegion=argv[1])

if __name__ == "__main__":
    sys.exit(main(sys.argv))

