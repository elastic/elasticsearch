module org.elasticsearch.kql {
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.antlr.antlr4.runtime;
    requires org.elasticsearch.base;
    requires org.apache.lucene.queryparser;
    requires org.elasticsearch.logging;
    requires org.apache.lucene.core;

    exports org.elasticsearch.xpack.kql;
    exports org.elasticsearch.xpack.kql.parser;
}
