/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * PreProcessor for extracting top level domain and second level domain information while taking into account a list of dynamic domains
 */
public class DynamicDomainFeatureExtractor implements LenientlyParsedPreProcessor, StrictlyParsedPreProcessor {

    private static final String TLD_STRING = "tld";
    private static final String SLD_STRING = "sld";

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(DynamicDomainFeatureExtractor.class);
    public static final ParseField NAME = new ParseField("dynamic_domain_feature_extractor");
    public static final ParseField DOMAIN_FIELD = new ParseField("domain_field");
    public static final ParseField REGISTERED_DOMAIN_FIELD = new ParseField("registered_domain_field");
    public static final ParseField SUBDOMAIN_FIELD = new ParseField("subdomain_field");
    public static final ParseField TOP_LEVEL_DOMAIN_FIELD = new ParseField("top_level_domain_field");
    public static final ParseField FEATURE_PREFIX = new ParseField("feature_prefix");
    public static final ParseField DYNAMIC_DOMAINS = new ParseField("dynamic_domains");

    private static final ConstructingObjectParser<DynamicDomainFeatureExtractor, PreProcessorParseContext> STRICT_PARSER =
        createParser(false);
    private static final ConstructingObjectParser<DynamicDomainFeatureExtractor, PreProcessorParseContext> LENIENT_PARSER =
        createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<DynamicDomainFeatureExtractor, PreProcessorParseContext> createParser(boolean lenient) {
        ConstructingObjectParser<DynamicDomainFeatureExtractor, PreProcessorParseContext> parser = new ConstructingObjectParser<>(
            NAME.getPreferredName(),
            lenient,
            (a, c) -> new DynamicDomainFeatureExtractor((String)a[0],
                (String)a[1],
                (String)a[2],
                (String)a[3],
                (String)a[4],
                new HashSet<>((List<String>)a[5])));
        parser.declareString(ConstructingObjectParser.constructorArg(), DOMAIN_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), REGISTERED_DOMAIN_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), SUBDOMAIN_FIELD);
        parser.declareString(ConstructingObjectParser.constructorArg(), TOP_LEVEL_DOMAIN_FIELD);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), FEATURE_PREFIX);
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), DYNAMIC_DOMAINS);
        return parser;
    }

    public static DynamicDomainFeatureExtractor fromXContentStrict(XContentParser parser, PreProcessorParseContext context) {
        return STRICT_PARSER.apply(parser, context == null ?  PreProcessorParseContext.DEFAULT : context);
    }

    public static DynamicDomainFeatureExtractor fromXContentLenient(XContentParser parser, PreProcessorParseContext context) {
        return LENIENT_PARSER.apply(parser, context == null ?  PreProcessorParseContext.DEFAULT : context);
    }

    private final String domainField;
    private final String registeredDomainField;
    private final String subDomainField;
    private final String topLevelDomainField;
    private final String featurePrefix;
    private final Set<String> dynamicDomains;

    public DynamicDomainFeatureExtractor(String domainField,
                                         String registeredDomainField,
                                         String subDomainField,
                                         String topLevelDomainField,
                                         String featurePrefix,
                                         Set<String> dynamicDomains) {
        this.domainField = ExceptionsHelper.requireNonNull(domainField, DOMAIN_FIELD);
        this.registeredDomainField = ExceptionsHelper.requireNonNull(registeredDomainField, REGISTERED_DOMAIN_FIELD);
        this.subDomainField = ExceptionsHelper.requireNonNull(subDomainField, SUBDOMAIN_FIELD);
        this.topLevelDomainField = ExceptionsHelper.requireNonNull(topLevelDomainField, TOP_LEVEL_DOMAIN_FIELD);
        this.featurePrefix = featurePrefix == null ? NAME.getPreferredName() : featurePrefix;
        this.dynamicDomains = dynamicDomains == null ? Collections.emptySet() : dynamicDomains;
    }

    public DynamicDomainFeatureExtractor(StreamInput in) throws IOException {
        this.domainField = in.readString();
        this.registeredDomainField = in.readString();
        this.subDomainField = in.readString();
        this.topLevelDomainField = in.readString();
        this.featurePrefix = in.readString();
        this.dynamicDomains = in.readSet(StreamInput::readString);
    }

    public String getDomainField() {
        return domainField;
    }

    public String getRegisteredDomainField() {
        return registeredDomainField;
    }

    public String getSubDomainField() {
        return subDomainField;
    }

    public String getTopLevelDomainField() {
        return topLevelDomainField;
    }

    public String getFeaturePrefix() {
        return featurePrefix;
    }

    public Set<String> getDynamicDomains() {
        return dynamicDomains;
    }

    @Override
    public boolean isCustom() {
        return true;
    }

    @Override
    public String getOutputFieldType(String outputField) {
        return KeywordFieldMapper.CONTENT_TYPE;
    }

    @Override
    public String getName() {
        return NAME.getPreferredName();
    }

    @Override
    public List<String> inputFields() {
        return Arrays.asList(domainField, registeredDomainField, subDomainField, topLevelDomainField);
    }

    @Override
    public List<String> outputFields() {
        return Stream.of(TLD_STRING, SLD_STRING).map(f -> featurePrefix + "." + f).collect(Collectors.toList());
    }

    @Override
    public void process(Map<String, Object> fields) {
        final String domain = getString(domainField, fields);
        final String registeredDomain = getString(registeredDomainField, fields);
        if (registeredDomain == null) {
            if (domain == null) {
                return;
            }
            fields.put(featurePrefix + "." + SLD_STRING, domain);
            fields.put(featurePrefix + "." + TLD_STRING, "");
            return;
        }

        final String subdomain = getString(subDomainField, fields);
        if (dynamicDomains.contains(registeredDomain) && subdomain != null) {
            fields.put(featurePrefix + "." + SLD_STRING, subdomain);
            fields.put(featurePrefix + "." + TLD_STRING, registeredDomain);
            return;
        }

        final String topLevelDomain = getString(topLevelDomainField, fields);
        if (topLevelDomain != null) {
            fields.put(featurePrefix + "." + SLD_STRING,
                registeredDomain.substring(0, registeredDomain.length() - topLevelDomain.length() - 1));
            fields.put(featurePrefix + "." + TLD_STRING, topLevelDomain);
        }
    }

    @Override
    public Map<String, String> reverseLookup() {
        throw new UnsupportedOperationException("["
            + NAME.getPreferredName()
            + "] does not support feature name reverse lookup. Required for original doc field feature importance.");
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.domainField);
        out.writeString(this.registeredDomainField);
        out.writeString(this.subDomainField);
        out.writeString(this.topLevelDomainField);
        out.writeString(this.featurePrefix);
        out.writeStringCollection(this.dynamicDomains);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DOMAIN_FIELD.getPreferredName(), domainField);
        builder.field(REGISTERED_DOMAIN_FIELD.getPreferredName(), registeredDomainField);
        builder.field(SUBDOMAIN_FIELD.getPreferredName(), subDomainField);
        builder.field(TOP_LEVEL_DOMAIN_FIELD.getPreferredName(), topLevelDomainField);
        builder.field(DYNAMIC_DOMAINS.getPreferredName(), dynamicDomains);
        builder.field(FEATURE_PREFIX.getPreferredName(), featurePrefix);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DynamicDomainFeatureExtractor that = (DynamicDomainFeatureExtractor) o;
        return Objects.equals(domainField, that.domainField) &&
            Objects.equals(registeredDomainField, that.registeredDomainField) &&
            Objects.equals(subDomainField, that.subDomainField) &&
            Objects.equals(topLevelDomainField, that.topLevelDomainField) &&
            Objects.equals(featurePrefix, that.featurePrefix) &&
            Objects.equals(dynamicDomains, that.dynamicDomains);
    }

    @Override
    public int hashCode() {
        return Objects.hash(domainField, registeredDomainField, subDomainField, topLevelDomainField, featurePrefix, dynamicDomains);
    }

    @Override
    public long ramBytesUsed() {
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.sizeOf(domainField);
        size += RamUsageEstimator.sizeOf(registeredDomainField);
        size += RamUsageEstimator.sizeOf(subDomainField);
        size += RamUsageEstimator.sizeOf(topLevelDomainField);
        size += RamUsageEstimator.sizeOf(featurePrefix);
        size += RamUsageEstimator.sizeOfCollection(dynamicDomains);
        return size;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    private static String getString(String field, Map<String, Object> stringObjectMap) {
        Object value = stringObjectMap.get(field);
        return value == null ? null : value.toString();
    }
}
