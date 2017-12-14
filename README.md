# rosbag_republisher
Republishes any messages found in a rosbag file. The major flaw with rosbag play is that it does not use the current time in the ros network. This script uses the current network time and publishes the messages in the bag file as if they were being broadcast normally now.  

## Launch Configuration
```xml
<node pkg="rosbag_republisher" type="rosbag_republisher.py" name="rosbag_republisher" output="screen" args="$(arg ros_bag_filename)" />
```
