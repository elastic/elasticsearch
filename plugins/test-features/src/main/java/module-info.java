module org.elasticsearch.features.plugin {
    requires org.elasticsearch.server;
    requires org.elasticsearch.logging;

    uses org.elasticsearch.features.FeatureSpecification;

    provides org.elasticsearch.features.FeatureSpecification with org.elasticsearch.features.plugin.TestFeaturesForwarderSpecification;
}
