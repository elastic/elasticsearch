module org.elasticsearch.injection {
    exports org.elasticsearch.injection.api;
    exports org.elasticsearch.injection.exceptions;
    exports org.elasticsearch.injection;

    requires org.elasticsearch.logging;
}
