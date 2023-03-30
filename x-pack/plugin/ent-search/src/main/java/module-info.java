module org.elasticsearch.application {
    requires org.apache.lucene.core;

    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;

    exports org.elasticsearch.xpack.application.analytics;
    exports org.elasticsearch.xpack.application.analytics.action;

    exports org.elasticsearch.xpack.application.search;
    exports org.elasticsearch.xpack.application.search.action;
}
