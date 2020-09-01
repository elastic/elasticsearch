/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.inference.preprocessing;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;


/**
 * PreProcessor for extracting top level domain and second level domain information while taking into account a list of dynamic domains
 */
public class DynamicDomainFeatureExtractor implements PreProcessor {

    public static final String NAME = "dynamic_domain_feature_extractor";
    public static final ParseField DOMAIN_FIELD = new ParseField("domain_field");
    public static final ParseField REGISTERED_DOMAIN_FIELD = new ParseField("registered_domain_field");
    public static final ParseField SUBDOMAIN_FIELD = new ParseField("subdomain_field");
    public static final ParseField TOP_LEVEL_DOMAIN_FIELD = new ParseField("top_level_domain_field");
    public static final ParseField FEATURE_PREFIX = new ParseField("feature_prefix");
    public static final ParseField DYNAMIC_DOMAINS = new ParseField("dynamic_domains");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DynamicDomainFeatureExtractor, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        (a, c) -> new DynamicDomainFeatureExtractor((String)a[0],
            (String)a[1],
            (String)a[2],
            (String)a[3],
            (String)a[4],
            new HashSet<>((List<String>)a[5])));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DOMAIN_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REGISTERED_DOMAIN_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SUBDOMAIN_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TOP_LEVEL_DOMAIN_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FEATURE_PREFIX);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), DYNAMIC_DOMAINS);
    }

    public static DynamicDomainFeatureExtractor fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String domainField;
    private final String registeredDomainField;
    private final String subDomainField;
    private final String topLevelDomainField;
    private final String featurePrefix;
    private final Set<String> dynamicDomains;

    DynamicDomainFeatureExtractor(String domainField,
                                  String registeredDomainField,
                                  String subDomainField,
                                  String topLevelDomainField,
                                  String featurePrefix,
                                  Set<String> dynamicDomains) {
        this.domainField = domainField;
        this.registeredDomainField = registeredDomainField;
        this.subDomainField = subDomainField;
        this.topLevelDomainField = topLevelDomainField;
        this.featurePrefix = featurePrefix;
        this.dynamicDomains = dynamicDomains;
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
    public String getName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
    public String toString() {
        return Strings.toString(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String domainField;
        private String registeredDomainField;
        private String subDomainField;
        private String topLevelDomainField;
        private String featurePrefix;
        private Set<String> dynamicDomains;

        public Builder setDomainField(String domainField) {
            this.domainField = domainField;
            return this;
        }

        public Builder setRegisteredDomainField(String registeredDomainField) {
            this.registeredDomainField = registeredDomainField;
            return this;
        }

        public Builder setSubDomainField(String subDomainField) {
            this.subDomainField = subDomainField;
            return this;
        }

        public Builder setTopLevelDomainField(String topLevelDomainField) {
            this.topLevelDomainField = topLevelDomainField;
            return this;
        }

        public Builder setFeaturePrefix(String featurePrefix) {
            this.featurePrefix = featurePrefix;
            return this;
        }

        public Builder setDynamicDomains(Set<String> dynamicDomains) {
            this.dynamicDomains = dynamicDomains;
            return this;
        }

        DynamicDomainFeatureExtractor build() {
            return new DynamicDomainFeatureExtractor(domainField,
                registeredDomainField,
                subDomainField,
                topLevelDomainField,
                featurePrefix,
                dynamicDomains);
        }
    }

}
