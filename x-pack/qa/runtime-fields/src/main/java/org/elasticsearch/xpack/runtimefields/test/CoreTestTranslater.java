/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.runtimefields.test;

import org.elasticsearch.action.bulk.BulkRequestParser;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.ApiCallSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSuite;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.test.rest.yaml.section.SetupSection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Builds test parameters similarly to {@link ESClientYamlSuiteTestCase#createParameters()},
 * replacing all fields with runtime fields that load from {@code _source} if possible. Tests
 * that configure the field in a way that are not supported by runtime fields are skipped.
 */
public abstract class CoreTestTranslater {
    public Iterable<Object[]> parameters() throws Exception {
        Map<String, Suite> suites = new HashMap<>();
        List<Object[]> result = new ArrayList<>();
        for (Object[] orig : ESClientYamlSuiteTestCase.createParameters()) {
            assert orig.length == 1;
            ClientYamlTestCandidate candidate = (ClientYamlTestCandidate) orig[0];
            Suite suite = suites.computeIfAbsent(candidate.getSuitePath(), k -> suite(candidate));
            if (suite.modified == null) {
                // The setup section contains an unsupported option
                continue;
            }
            if (false == suite.modifySections(candidate.getTestSection().getExecutableSections())) {
                // The test section contains an unsupported option
                continue;
            }
            ClientYamlTestSection modified = new ClientYamlTestSection(
                candidate.getTestSection().getLocation(),
                candidate.getTestSection().getName(),
                candidate.getTestSection().getSkipSection(),
                candidate.getTestSection().getExecutableSections()
            );
            result.add(new Object[] { new ClientYamlTestCandidate(suite.modified, modified) });
        }
        return result;
    }

    protected abstract Suite suite(ClientYamlTestCandidate candidate);

    private static final Set<String> RUNTIME_TYPES = Set.of(
        BooleanFieldMapper.CONTENT_TYPE,
        DateFieldMapper.CONTENT_TYPE,
        NumberType.DOUBLE.typeName(),
        KeywordFieldMapper.CONTENT_TYPE,
        IpFieldMapper.CONTENT_TYPE,
        GeoPointFieldMapper.CONTENT_TYPE,
        NumberType.LONG.typeName()
    );

    protected abstract Map<String, Object> dynamicTemplateFor();

    protected static Map<String, Object> dynamicTemplateToDisableRuntimeCompatibleFields() {
        return Map.of("mapping", Map.of("index", false, "doc_values", false));
    }

    protected static Map<String, Object> dynamicTemplateToAddRuntimeFields() {
        return Map.of("runtime", Map.of());
    }

    protected static Map<String, Object> runtimeFieldLoadingFromSource(String type) {
        return Map.of("type", type);
    }

    private ExecutableSection addIndexTemplate() {
        return new ExecutableSection() {
            @Override
            public XContentLocation getLocation() {
                return new XContentLocation(-1, -1);
            }

            @Override
            public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
                Map<String, String> params = Map.of("name", "hack_dynamic_mappings", "create", "true");
                List<Map<String, Object>> dynamicTemplates = new ArrayList<>();
                for (String type : RUNTIME_TYPES) {
                    /*
                     * It would be great to use dynamic:runtime rather than dynamic templates.
                     * Unfortunately, string gets dynamically mapped as a multi-field (text + keyword) which we can't mimic as
                     * runtime fields don't support text, and from a dynamic template a field can either be runtime or concrete.
                     * We would like to define a keyword sub-field under runtime and leave the main field under properties but that
                     * is not possible. What we do for now is skip strings: we register a dynamic template for each type besides string.
                     * Ip and geo_point fields never get dynamically mapped so they'll just look like strings.
                     */
                    if (type.equals(IpFieldMapper.CONTENT_TYPE)
                        || type.equals(GeoPointFieldMapper.CONTENT_TYPE)
                        || type.equals(KeywordFieldMapper.CONTENT_TYPE)) {
                        continue;
                    }
                    HashMap<String, Object> map = new HashMap<>();
                    map.put("match_mapping_type", type);
                    map.putAll(dynamicTemplateFor());
                    dynamicTemplates.add(Map.of(type, map));
                }
                Map<String, Object> indexTemplate = Map.of("settings", Map.of(), "mappings", Map.of("dynamic_templates", dynamicTemplates));
                List<Map<String, Object>> bodies = List.of(
                    Map.ofEntries(
                        Map.entry("index_patterns", "*"),
                        Map.entry("priority", Integer.MAX_VALUE - 1),
                        Map.entry("template", indexTemplate)
                    )
                );
                ClientYamlTestResponse response = executionContext.callApi("indices.put_index_template", params, bodies, Map.of());
                assertThat(response.getStatusCode(), equalTo(200));
                // There are probably some warning about overlapping templates. Ignore them.
            }
        };
    }

    /**
     * A modified suite.
     */
    protected abstract class Suite {
        private final ClientYamlTestSuite modified;

        public Suite(ClientYamlTestCandidate candidate) {
            if (false == modifySections(candidate.getSetupSection().getExecutableSections())) {
                modified = null;
                return;
            }
            /*
             * Modify the setup section to rewrite and create index commands and
             * to add a dynamic template that sets up any dynamic indices how we
             * expect them.
             */
            List<ExecutableSection> setup = new ArrayList<>(candidate.getSetupSection().getExecutableSections().size() + 1);
            setup.add(addIndexTemplate());
            setup.addAll(candidate.getSetupSection().getExecutableSections());
            modified = new ClientYamlTestSuite(
                candidate.getApi(),
                candidate.getName(),
                new SetupSection(candidate.getSetupSection().getSkipSection(), setup),
                candidate.getTeardownSection(),
                List.of()
            );
        }

        /**
         * Replace field configuration in {@code indices.create} with scripts
         * that load from the source.
         *
         * @return true if the section is appropriate for testing with runtime fields
         */
        public boolean modifySections(List<ExecutableSection> executables) {
            for (ExecutableSection section : executables) {
                if (false == (section instanceof DoSection)) {
                    continue;
                }
                DoSection doSection = (DoSection) section;
                String api = doSection.getApiCallSection().getApi();
                switch (api) {
                    case "indices.create":
                        if (false == modifyCreateIndex(doSection.getApiCallSection())) {
                            return false;
                        }
                        break;
                    case "search":
                    case "async_search.submit":
                        if (false == modifySearch(doSection.getApiCallSection())) {
                            return false;
                        }
                        break;
                    case "bulk":
                        if (false == handleBulk(doSection.getApiCallSection())) {
                            return false;
                        }
                        break;
                    case "index":
                        if (false == handleIndex(doSection.getApiCallSection())) {
                            return false;
                        }
                        break;
                    default:
                        continue;
                }
            }
            return true;
        }

        /**
         * Modify a test search request.
         */
        protected abstract boolean modifySearch(ApiCallSection search);

        private boolean modifyCreateIndex(ApiCallSection createIndex) {
            String index = createIndex.getParams().get("index");
            for (Map<?, ?> body : createIndex.getBodies()) {
                Object settings = body.get("settings");
                if (settings instanceof Map && ((Map<?, ?>) settings).containsKey("sort.field")) {
                    /*
                     * You can't sort the index on a runtime field
                     */
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> mapping = (Map<String, Object>) body.get("mappings");
                if (mapping == null) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> propertiesMap = (Map<String, Object>) ((Map<?, ?>) mapping).get("properties");
                if (propertiesMap == null) {
                    continue;
                }
                Map<String, Object> runtimeFields = new HashMap<>();
                if (false == modifyMappingProperties(index, propertiesMap, runtimeFields)) {
                    return false;
                }
                mapping.put("runtime", runtimeFields);
            }
            return true;
        }

        /**
         * Modify the mapping defined in the test.
         */
        protected abstract boolean modifyMappingProperties(String index, Map<String, Object> mappings, Map<String, Object> runtimeFields);

        /**
         * Modify the provided map in place, translating all fields into
         * runtime fields that load from source.
         * @return true if this mapping supports runtime fields, false otherwise
         */
        protected final boolean runtimeifyMappingProperties(Map<String, Object> properties, Map<String, Object> runtimeFields) {
            for (Map.Entry<String, Object> property : properties.entrySet()) {
                if (false == property.getValue() instanceof Map) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> propertyMap = (Map<String, Object>) property.getValue();
                String name = property.getKey();
                String type = Objects.toString(propertyMap.get("type"));
                if ("nested".equals(type)) {
                    // Our loading scripts can't be made to manage nested fields so we have to skip those tests.
                    return false;
                }
                if ("false".equals(Objects.toString(propertyMap.get("doc_values")))) {
                    // If doc_values is false we can't emulate with scripts. So we keep the old definition. `null` and `true` are fine.
                    continue;
                }
                if ("false".equals(Objects.toString(propertyMap.get("index")))) {
                    // If index is false we can't emulate with scripts
                    continue;
                }
                if ("true".equals(Objects.toString(propertyMap.get("store")))) {
                    // If store is true we can't emulate with scripts
                    continue;
                }
                if (propertyMap.containsKey("ignore_above")) {
                    // Scripts don't support ignore_above so we skip those fields
                    continue;
                }
                if (propertyMap.containsKey("ignore_malformed")) {
                    // Our source reading script doesn't emulate ignore_malformed
                    continue;
                }
                if (RUNTIME_TYPES.contains(type) == false) {
                    continue;
                }
                Map<String, Object> runtimeConfig = new HashMap<>(propertyMap);
                runtimeConfig.put("type", type);
                runtimeConfig.remove("store");
                runtimeConfig.remove("index");
                runtimeConfig.remove("doc_values");
                runtimeFields.put(name, runtimeConfig);

                // we disable the mapped fields and shadow them with their corresponding runtime field
                propertyMap.put("doc_values", false);
                propertyMap.put("index", false);
            }
            /*
             * Its tempting to return false here if we didn't make any runtime
             * fields, skipping the test. But that would cause us to skip any
             * test uses dynamic mappings. Disaster! Instead we use a dynamic
             * template to make the dynamic mappings into runtime fields too.
             * The downside is that we can run tests that don't use runtime
             * fields at all. That's unfortunate, but its ok.
             */
            return true;
        }

        private boolean handleBulk(ApiCallSection bulk) {
            String defaultIndex = bulk.getParams().get("index");
            String defaultRouting = bulk.getParams().get("routing");
            String defaultPipeline = bulk.getParams().get("pipeline");
            BytesStreamOutput bos = new BytesStreamOutput();
            try {
                for (Map<String, Object> body : bulk.getBodies()) {
                    try (XContentBuilder b = new XContentBuilder(JsonXContent.jsonXContent, bos)) {
                        b.map(body);
                    }
                    bos.write(JsonXContent.jsonXContent.streamSeparator());
                }
                List<IndexRequest> indexRequests = new ArrayList<>();
                new BulkRequestParser(false, RestApiVersion.current()).parse(
                    bos.bytes(),
                    defaultIndex,
                    defaultRouting,
                    null,
                    defaultPipeline,
                    null,
                    true,
                    XContentType.JSON,
                    (index, type) -> indexRequests.add(index),
                    u -> {},
                    d -> {}
                );
                for (IndexRequest index : indexRequests) {
                    if (false == handleIndex(index)) {
                        return false;
                    }
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            return true;
        }

        private boolean handleIndex(ApiCallSection indexRequest) {
            String index = indexRequest.getParams().get("index");
            String pipeline = indexRequest.getParams().get("pipeline");
            assert indexRequest.getBodies().size() == 1;
            try {
                return handleIndex(new IndexRequest(index).setPipeline(pipeline).source(indexRequest.getBodies().get(0)));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        protected abstract boolean handleIndex(IndexRequest index) throws IOException;
    }
}
