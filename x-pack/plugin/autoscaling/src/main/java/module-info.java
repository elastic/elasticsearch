module org.elasticsearch.autoscaling {
    requires org.apache.logging.log4j;
    requires org.apache.lucene.core;

    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;

    exports org.elasticsearch.xpack.autoscaling.action;
    exports org.elasticsearch.xpack.autoscaling.capacity;
    exports org.elasticsearch.xpack.autoscaling;
}
