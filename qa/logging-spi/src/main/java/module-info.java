
module org.elasticsearch.internal.security {

    requires org.elasticsearch.server;

    provides org.elasticsearch.plugins.internal.LoggingDataProvider with org.elasticsearch.test.logging.plugin.CustomDataProvider;
}
