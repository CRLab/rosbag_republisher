#!/usr/bin/python

import rospy
import rosbag
import argparse

from bond.msg import Status
from sensor_msgs.msg import Image, CompressedImage, PointCloud2, CameraInfo
import tf


# Mapping from topic name to message type, add more as needed
topics = [
    ("/kinect2/bond", Status),
    ("/kinect2/hd/camera_info", CameraInfo),
    ("/kinect2/hd/image_color", Image),
    ("/kinect2/hd/image_color/compressed", CompressedImage),
    ("/kinect2/hd/image_color_rect", Image),
    ("/kinect2/hd/image_color_rect/compressed", CompressedImage),
    ("/kinect2/hd/image_depth_rect", Image),
    ("/kinect2/hd/image_depth_rect/compressed", CompressedImage),
    ("/kinect2/hd/image_mono", Image),
    ("/kinect2/hd/image_mono/compressed", CompressedImage),
    ("/kinect2/hd/image_mono_rect", Image),
    ("/kinect2/hd/image_mono_rect/compressed", CompressedImage),
    ("/kinect2/hd/points", PointCloud2),
    ("/kinect2/qhd/camera_info", CameraInfo),
    ("/kinect2/qhd/image_color", Image),
    ("/kinect2/qhd/image_color/compressed", CompressedImage),
    ("/kinect2/qhd/image_color_rect", Image),
    ("/kinect2/qhd/image_color_rect/compressed", CompressedImage),
    ("/kinect2/qhd/image_depth_rect", Image),
    ("/kinect2/qhd/image_depth_rect/compressed", CompressedImage),
    ("/kinect2/qhd/image_mono", Image),
    ("/kinect2/qhd/image_mono/compressed", CompressedImage),
    ("/kinect2/qhd/image_mono_rect", Image),
    ("/kinect2/qhd/image_mono_rect/compressed", CompressedImage),
    ("/kinect2/qhd/points", PointCloud2),
    ("/kinect2/sd/camera_info", CameraInfo),
    ("/kinect2/sd/image_color_rect", Image),
    ("/kinect2/sd/image_color_rect/compressed", CompressedImage),
    ("/kinect2/sd/image_depth", Image),
    ("/kinect2/sd/image_depth/compressed", CompressedImage),
    ("/kinect2/sd/image_depth_rect", Image),
    ("/kinect2/sd/image_depth_rect/compressed", CompressedImage),
    ("/kinect2/sd/image_ir", Image),
    ("/kinect2/sd/image_ir/compressed", CompressedImage),
    ("/kinect2/sd/image_ir_rect", Image),
    ("/kinect2/sd/image_ir_rect/compressed", CompressedImage),
    ("/kinect2/sd/points", PointCloud2)
]


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


def sort_msgs(msgs):
    """ Messages sorted by timestamp in rosbag """
    return sorted(msgs, cmp=lambda x, y: x[2] < y[2])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("bag_file", help="Filepath to bagfile")
    return parser.parse_args()


def main():
    """ Takes in a rosbag and republishes the messages as if they were sent now
        Rosbag play does not republish messages using wall time, which is frustrating because it resets
        the overall clock when the messages are replayed. This program takes in a rosbag and plays it
        by reading each message and publishing it by updating the time.
        In order to update this file you'd need to add additional topics to the topics list. This program
        could eventually be parameterized better. """
    args = parse_args()
    rospy.init_node("kinect2_republisher")

    # Initialize all publishers for topics defined at the top of the file
    publishers = {}
    for topic, msg in topics:
        publishers[topic] = rospy.Publisher(topic, msg, queue_size=10)

    # tf is a special case, much less complicated to use a tf broadcaster
    tf_publisher = tf.TransformBroadcaster()

    # Read in the bag and sort the messages by time
    bag = rosbag.Bag(args.bag_file)
    sorted_messages = sort_msgs(bag.read_messages())

    previous_t = sorted_messages[0][2]

    while not rospy.is_shutdown():
        t_start = rospy.Time.now()

        for topic, msg, t in sorted_messages:
            if topic in publishers:
                publish_msg(publishers[topic], msg)
            if topic == "/tf":
                for transform_msg in msg.transforms:
                    publish_transform(tf_publisher, transform_msg)

            if rospy.is_shutdown():
                break

            rospy.sleep(t - previous_t)
            previous_t = t

        # Make sure the time taken is roughly the time that is in the bag. This program tends to run a bit longer
        print("Sent all messages in {} seconds".format((rospy.Time.now() - t_start).to_sec()))
        previous_t = sorted_messages[0][2]


if __name__ == '__main__':
    main()
