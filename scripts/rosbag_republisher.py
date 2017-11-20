#!/usr/bin/python

import rospy
import rosbag
import argparse
from pydoc import locate

import sys
import tf


def publish_transform(tf_publisher, transform_msg):
    """ Update timestamp of transform and publish it as if it were a new message """
    translation = transform_msg.transform.translation
    rotation = transform_msg.transform.rotation
    tf_publisher.sendTransform((translation.x, translation.y, translation.z),
                               (rotation.x, rotation.y, rotation.z, rotation.w),
                               rospy.Time.now(),
                               transform_msg.child_frame_id,
                               transform_msg.header.frame_id)


def publish_msg(publisher, msg):
    """ Update timestamp on messages that have a header and publish the message """
    if hasattr(msg, "header"):
        msg.header.stamp = rospy.Time.now()
    publisher.publish(msg)


def read_topic_types_from_bag(bag):
    topic_types = []
    for i in range(0, len(bag.get_type_and_topic_info()[1].values())):
        topic_type = locate(bag.get_type_and_topic_info()[1].values()[i][0].replace('/', '.msg.'))
        topic = bag.get_type_and_topic_info()[1].keys()[i]
        topic_types.append((topic, topic_type))
    return topic_types


def sort_msgs(msgs):
    """ Messages sorted by timestamp in rosbag """
    return sorted(msgs, cmp=lambda x, y: x[2] < y[2])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("bag_file", help="Filepath to bagfile")

    args_list = " ".join(sys.argv[1:]).replace("__", "--").replace(":=", " ").split(" ")

    args, _ = parser.parse_known_args(args_list)
    return args


def main():
    """ Takes in a rosbag and republishes the messages as if they were sent now
        Rosbag play does not republish messages using wall time, which is frustrating because it resets
        the overall clock when the messages are replayed. This program takes in a rosbag and plays it
        by reading each message and publishing it by updating the time. The types that are stored in the bagfile
        need to be accessible via python. """
    args = parse_args()
    rospy.init_node("rosbag_republisher")

    # Read in the bag and sort the messages by time
    bag = rosbag.Bag(args.bag_file)
    topic_types = read_topic_types_from_bag(bag)
    sorted_messages = sort_msgs(bag.read_messages())

    # Initialize all publishers for topics defined at the top of the file
    publishers = {}
    for topic, topic_type in topic_types:
        publishers[topic] = rospy.Publisher(topic, topic_type, queue_size=10)

    # tf is a special case, much less complicated to use a tf broadcaster
    tf_publisher = tf.TransformBroadcaster()

    previous_t = sorted_messages[0][2]

    while not rospy.is_shutdown():
        t_start = rospy.Time.now()

        for topic, msg, t in sorted_messages:
            if topic == "/tf":
                for transform_msg in msg.transforms:
                    publish_transform(tf_publisher, transform_msg)
            elif topic in publishers:
                publish_msg(publishers[topic], msg)

            if rospy.is_shutdown():
                break

            rospy.sleep(t - previous_t)
            previous_t = t

        # Make sure the time taken is roughly the time that is in the bag. This program tends to run a bit longer\
        rospy.loginfo("[rosbag_republisher] Sent all messages in {} seconds".format((rospy.Time.now() - t_start).to_sec()))
        previous_t = sorted_messages[0][2]


if __name__ == '__main__':
    main()
