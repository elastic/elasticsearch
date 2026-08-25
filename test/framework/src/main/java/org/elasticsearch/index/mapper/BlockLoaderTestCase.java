/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.DocumentGenerator;
import org.elasticsearch.datageneration.Mapping;
import org.elasticsearch.datageneration.MappingGenerator;
import org.elasticsearch.datageneration.Template;
import org.elasticsearch.datageneration.datasource.DataSourceHandler;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.datasource.DataSourceResponse;
import org.elasticsearch.datageneration.datasource.DefaultMappingParametersHandler;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test case for {@link MappedFieldType#blockLoader} specializing in configuring the mapping and
 * invoking {@link BlockLoader}. See {@link AbstractBlockLoaderTestCase} for tests that don't
 * require setting up a field mapper.
 *
 * TODO rename this.
 */
public abstract class BlockLoaderTestCase extends MapperServiceTestCase {
    protected static final MappedFieldType.FieldExtractPreference[] PREFERENCES = new MappedFieldType.FieldExtractPreference[] {
        MappedFieldType.FieldExtractPreference.NONE,
        MappedFieldType.FieldExtractPreference.DOC_VALUES,
        MappedFieldType.FieldExtractPreference.STORED };

    protected static final SourceFieldMapper.Mode[] SOURCE_MODES = {
        SourceFieldMapper.Mode.STORED,
        SourceFieldMapper.Mode.SYNTHETIC,
        SourceFieldMapper.Mode.COLUMNAR_STORED };

    /**
     * A large enough size that loading the field won't circuit break.
     */
    public static final ByteSizeValue TEST_BREAKER_SIZE = ByteSizeValue.ofMb(1);

    @ParametersFactory(argumentFormatting = "preference=%s")
    public static List<Object[]> args() {
        List<Object[]> args = new ArrayList<>();

        List<IndexMode> modes = List.of(IndexMode.STANDARD, IndexMode.COLUMNAR);

        for (IndexMode indexMode : modes) {
            for (SourceFieldMapper.Mode sourceMode : SOURCE_MODES) {
                if (indexMode.supportedSourceModes().contains(sourceMode) == false) {
                    continue;
                }
                for (MappedFieldType.FieldExtractPreference preference : PREFERENCES) {
                    args.add(new Object[] { new Params(indexMode, sourceMode, preference) });
                }
            }
        }
        return args;
    }

    public record Params(IndexMode indexMode, SourceFieldMapper.Mode sourceMode, MappedFieldType.FieldExtractPreference preference) {
        public boolean syntheticSource() {
            return sourceMode == SourceFieldMapper.Mode.SYNTHETIC;
        }

        public boolean isColumnarStored() {
            return sourceMode == SourceFieldMapper.Mode.COLUMNAR_STORED;
        }
    }

    public record TestContext(boolean forceFallbackSyntheticSource, boolean isMultifield) {}

    protected final String fieldType;
    protected final Params params;
    private final Collection<DataSourceHandler> customDataSourceHandlers;
    protected final BlockLoaderTestRunner runner;

    protected final String fieldName;

    protected BlockLoaderTestCase(String fieldType, Params params) {
        this(fieldType, List.of(), params);
    }

    protected BlockLoaderTestCase(String fieldType, Collection<DataSourceHandler> customDataSourceHandlers, Params params) {
        this.fieldType = fieldType;
        this.params = params;
        this.customDataSourceHandlers = withSingleValueDocValues(fieldType, customDataSourceHandlers, params.indexMode());
        this.runner = new BlockLoaderTestRunner(params);
        if (randomBoolean()) {
            runner.allowDummyDocs();
        }

        this.fieldName = randomAlphaOfLengthBetween(5, 10);
    }

    /**
     * Field types whose mappers enforce single-value semantics through {@code doc_values.multi_value: false}. Only these may carry the
     * parameter, so the coordinated single-value handler below is injected for them alone.
     */
    private static final Set<String> SINGLE_VALUE_ENFORCING_TYPES = Set.of(
        "keyword",
        "text",
        "match_only_text",
        "long",
        "integer",
        "short",
        "byte",
        "double",
        "float",
        "half_float",
        "unsigned_long",
        "scaled_float",
        "boolean",
        "date",
        "ip"
    );

    /**
     * On a random subset of runs (columnar mode only), prepend a handler that forces
     * {@code doc_values.multi_value: false} on the target field while keeping generated documents single-valued, so the enforced mapping
     * is exercised without rejecting documents.
     */
    private static Collection<DataSourceHandler> withSingleValueDocValues(
        String fieldType,
        Collection<DataSourceHandler> customHandlers,
        IndexMode indexMode
    ) {
        boolean singleValueRun = indexMode.isStrictColumnar()
            && SINGLE_VALUE_ENFORCING_TYPES.contains(fieldType)
            && ESTestCase.randomBoolean();
        if (singleValueRun == false) {
            return customHandlers;
        }
        // Prepend so the single-value array wrapper takes precedence over any default wrapper for this run.
        var handlers = new ArrayList<DataSourceHandler>();
        handlers.add(new SingleValueDocValuesDataSourceHandler());
        handlers.addAll(customHandlers);
        return handlers;
    }

    /**
     * Coordinated handler pairing two decisions that otherwise run independently: it forces {@code doc_values.multi_value: false} on
     * single-value-enforcing fields and, in lock-step, never wraps values into arrays of length two or more. Without the coupling the
     * enforced mapping would reject any document the array wrapper happened to make multi-valued.
     */
    private static final class SingleValueDocValuesDataSourceHandler implements DataSourceHandler {
        @Override
        public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
            if (SINGLE_VALUE_ENFORCING_TYPES.contains(request.fieldType()) == false) {
                // Leave non-enforcing fields (e.g. multi-field subfields of other types) to the default handlers.
                return null;
            }
            // Delegate directly to the default handler to keep all other generated parameters, then only rewrite doc_values.
            // This handler only ever runs when indexMode.isStrictColumnar() (see withSingleValueDocValues), so the delegate
            // must also know it's columnar-mode aware, or it could emit store/synthetic_source_keep/copy_to that are invalid there.
            var defaults = new DefaultMappingParametersHandler(IndexMode.COLUMNAR).handle(request);
            if (defaults == null) {
                return null;
            }
            return new DataSourceResponse.LeafMappingParametersGenerator(() -> {
                var mapping = new HashMap<>(defaults.mappingGenerator().get());
                mapping.put("doc_values", Map.of("multi_value", false));
                return mapping;
            });
        }

        @Override
        public DataSourceResponse.ArrayWrapper handle(DataSourceRequest.ArrayWrapper request) {
            // multi_value: false rejects documents with more than one value, so only ever emit a scalar, an empty array, or one element.
            return new DataSourceResponse.ArrayWrapper(values -> () -> {
                if (ESTestCase.randomBoolean()) {
                    var size = ESTestCase.randomIntBetween(0, 1);
                    return IntStream.range(0, size).mapToObj(i -> values.get()).toList();
                }
                return values.get();
            });
        }

        @Override
        public DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator request) {
            // Arrays of objects would repeat a leaf field across elements, producing multiple values for it and tripping the
            // single-value enforcement, so never wrap objects into arrays while this handler is active.
            return new DataSourceResponse.ObjectArrayGenerator(Optional::empty);
        }

    }

    @Override
    public void testFieldHasValue() {
        assumeTrue("random test inherited from MapperServiceTestCase", false);
    }

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {
        assumeTrue("random test inherited from MapperServiceTestCase", false);
    }

    protected String getFieldNameToLoad(String fieldName, Object value) {
        return fieldName;
    }

    public void testBlockLoader() throws IOException {
        testBlockLoader(newLimitedBreaker(TEST_BREAKER_SIZE));
    }

    public void testBlockLoaderWithCranky() throws IOException {
        CircuitBreaker cranky = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        try {
            testBlockLoader(cranky);
            logger.info("Cranky breaker didn't break. This should be rare, but possible randomly.");
        } catch (CircuitBreakingException e) {
            logger.info("Cranky breaker broke", e);
        }
        assertThat(cranky.getUsed(), equalTo(0L));
    }

    private void testBlockLoader(CircuitBreaker breaker) throws IOException {
        runner.breaker(breaker);
        var template = new Template(Map.of(fieldName, new Template.Leaf(fieldName, fieldType)));
        var specification = buildSpecification(customDataSourceHandlers, params.indexMode);

        var mapping = new MappingGenerator(specification).generate(template);
        runner.document(new DocumentGenerator(specification).generate(template, mapping));

        runner.fieldName(getFieldNameToLoad(fieldName, getFieldValue(runner.mapDoc(), fieldName)));

        Object expected = expected(
            mapping.lookup().get(fieldName),
            getFieldValue(runner.mapDoc(), runner.fieldName()),
            new TestContext(false, false)
        );

        var settings = getSettingsForParams();
        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());
        runner.mapperService(createMapperService(settings.build(), mappingXContent));
        configureRunner(runner, settings, mapping).run(expected);
    }

    protected BlockLoaderTestRunner configureRunner(BlockLoaderTestRunner runner, Settings.Builder settings, Mapping mapping) {
        return runner;
    }

    public void testBlockLoaderForFieldInObject() throws IOException {
        testBlockLoaderForFieldInObject(newLimitedBreaker(TEST_BREAKER_SIZE));
    }

    public void testBlockLoaderForFieldInObjectWithCranky() throws IOException {
        CircuitBreaker cranky = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        try {
            testBlockLoaderForFieldInObject(cranky);
            logger.info("Cranky breaker didn't break. This should be rare, but possible randomly.");
        } catch (CircuitBreakingException e) {
            logger.info("Cranky breaker broke", e);
        }
        assertThat(cranky.getUsed(), equalTo(0L));
    }

    @SuppressWarnings("unchecked")
    private void testBlockLoaderForFieldInObject(CircuitBreaker breaker) throws IOException {
        runner.breaker(breaker);
        int depth = randomIntBetween(0, 3);

        Map<String, Template.Entry> currentLevel = new HashMap<>();
        Map<String, Template.Entry> top = Map.of("top", new Template.Object("top", false, currentLevel));

        var fullFieldName = new StringBuilder("top");
        int currentDepth = 0;
        while (currentDepth++ < depth) {
            fullFieldName.append('.').append("level").append(currentDepth);

            Map<String, Template.Entry> nextLevel = new HashMap<>();
            currentLevel.put("level" + currentDepth, new Template.Object("level" + currentDepth, false, nextLevel));
            currentLevel = nextLevel;
        }

        fullFieldName.append('.').append(fieldName);
        currentLevel.put(fieldName, new Template.Leaf(fieldName, fieldType));
        var template = new Template(top);

        var specification = buildSpecification(customDataSourceHandlers, params.indexMode);
        var mapping = new MappingGenerator(specification).generate(template);
        runner.document(new DocumentGenerator(specification).generate(template, mapping));

        TestContext testContext = new TestContext(false, false);

        // synthetic_source_keep is rejected on strict-columnar indices, so the fallback-through-ignored-source path can only be exercised
        // on non-strict-columnar synthetic-source indices.
        if (params.syntheticSource() && params.indexMode.isStrictColumnar() == false && randomBoolean()) {
            // force fallback synthetic source in the hierarchy
            var docMapping = (Map<String, Object>) mapping.raw().get("_doc");
            var topLevelMapping = (Map<String, Object>) ((Map<String, Object>) docMapping.get("properties")).get("top");
            topLevelMapping.put("synthetic_source_keep", "all");

            testContext = new TestContext(true, false);
        }

        var settings = getSettingsForParams();
        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());
        runner.mapperService(createMapperService(settings.build(), mappingXContent));

        runner.fieldName(getFieldNameToLoad(fullFieldName.toString(), getFieldValue(runner.mapDoc(), fullFieldName.toString())));

        Object expected = expected(
            mapping.lookup().get(fullFieldName.toString()),
            getFieldValue(runner.mapDoc(), runner.fieldName()),
            testContext
        );

        configureRunner(runner, settings, mapping).run(expected);
    }

    protected boolean supportsMultiField() {
        return true;
    }

    public void testBlockLoaderOfMultiField() throws IOException {
        if (false == supportsMultiField()) {
            return;
        }
        testBlockLoaderOfMultiField(newLimitedBreaker(TEST_BREAKER_SIZE));
    }

    public void testBlockLoaderOfMultiFieldWithCranky() throws IOException {
        if (false == supportsMultiField()) {
            return;
        }
        CircuitBreaker cranky = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        try {
            testBlockLoaderOfMultiField(cranky);
            logger.info("Cranky breaker didn't break. This should be rare, but possible randomly.");
        } catch (CircuitBreakingException e) {
            logger.info("Cranky breaker broke", e);
        }
        assertThat(cranky.getUsed(), equalTo(0L));
    }

    private void testBlockLoaderOfMultiField(CircuitBreaker breaker) throws IOException {
        runner.breaker(breaker);
        // We are going to have a parent field and a multi field of the same type in order to be sure we can index data.
        // Then we'll test block loader of the multi field.
        var template = new Template(Map.of("parent", new Template.Leaf("parent", fieldType)));

        var customHandlers = new ArrayList<DataSourceHandler>();
        customHandlers.add(new DataSourceHandler() {
            @Override
            public DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
                // This is a bit tricky meta-logic.
                // We want to customize mapping but to do this we need the mapping for the same field type so we use name to untangle this.
                // In strict-columnar mode, the buildSpecification wrapper renames "parent" to "_columnar_inner_parent" when it recurses
                // through the handler chain; match that too so the {fields: {mf: …}} subtree still lands on the parent mapping.
                if (request.fieldName().equals("parent") == false && request.fieldName().equals("_columnar_inner_parent") == false) {
                    return null;
                }

                return new DataSourceResponse.LeafMappingParametersGenerator(() -> {
                    var dataSource = request.dataSource();

                    // We need parent field to have the same mapping as multi field due to different behavior caused f.e. by
                    // ignore_malformed.
                    // The name here should be different from "parent".
                    var mapping = dataSource.get(
                        new DataSourceRequest.LeafMappingParametersGenerator(
                            dataSource,
                            "_field",
                            request.fieldType(),
                            request.eligibleCopyToFields(),
                            request.dynamicMapping()
                        )
                    ).mappingGenerator().get();

                    var parentMapping = new HashMap<>(mapping);
                    var multiFieldMapping = new HashMap<>(mapping);

                    multiFieldMapping.put("type", fieldType);
                    multiFieldMapping.remove("fields");

                    parentMapping.put("fields", Map.of("mf", multiFieldMapping));

                    return parentMapping;
                });
            }
        });
        customHandlers.addAll(customDataSourceHandlers);
        var specification = buildSpecification(customHandlers, params.indexMode);
        var mapping = new MappingGenerator(specification).generate(template);
        @SuppressWarnings("unchecked")
        var fieldMapping = (Map<String, Object>) ((Map<String, Object>) mapping.lookup().get("parent").get("fields")).get("mf");

        runner.document(new DocumentGenerator(specification).generate(template, mapping));

        Object expected = expected(fieldMapping, getFieldValue(runner.mapDoc(), "parent"), new TestContext(false, true));
        var settings = getSettingsForParams();
        var mappingXContent = XContentBuilder.builder(XContentType.JSON.xContent()).map(mapping.raw());
        runner.fieldName("parent.mf");
        runner.mapperService(createMapperService(settings.build(), mappingXContent));
        configureRunner(runner, settings, mapping).run(expected);
    }

    protected Settings.Builder getSettingsForParams() {
        return getSettingsForParams(params);
    }

    public static Settings.Builder getSettingsForParams(Params params) {
        var builder = Settings.builder();
        builder.put("index.mapping.source.mode", params.sourceMode().name());
        builder.put(IndexSettings.MODE.getKey(), params.indexMode().name());
        return builder;
    }

    public static DataGeneratorSpecification buildSpecification(Collection<DataSourceHandler> customHandlers) {
        return buildSpecification(customHandlers, IndexMode.STANDARD);
    }

    public static DataGeneratorSpecification buildSpecification(Collection<DataSourceHandler> customHandlers, IndexMode indexMode) {
        var coreHandlers = new ArrayList<DataSourceHandler>();
        coreHandlers.add(new DataSourceHandler() {
            @Override
            public DataSourceResponse.DynamicMappingGenerator handle(DataSourceRequest.DynamicMappingGenerator request) {
                return new DataSourceResponse.DynamicMappingGenerator(isObject -> false);
            }

            @Override
            public DataSourceResponse.ObjectMappingParametersGenerator handle(DataSourceRequest.ObjectMappingParametersGenerator request) {
                return new DataSourceResponse.ObjectMappingParametersGenerator(HashMap::new); // just defaults
            }
        });
        return DataGeneratorSpecification.builder()
            .withFullyDynamicMapping(false)
            // Disable dynamic mapping and disabled objects
            .withDataSourceHandlers(coreHandlers)
            .withDataSourceHandlers(customHandlers)
            .withIndexMode(indexMode)
            .build();
    }

    /**
     * For a given mapping and input value, compute the value that will be in the block.  Values are generated from the
     * {@link DocumentGenerator}, and the behavior can be controled by writing a custom {@link DataSourceHandler}.
     *
     * @param fieldMapping Generated parameters for this field mapping
     * @param value Generated input value to convert
     * @param testContext Context information for the current test run
     * @return The value that will be added to the block
     */
    protected abstract Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext);

    protected static Object maybeFoldList(List<?> list) {
        if (list.isEmpty()) {
            return null;
        }

        if (list.size() == 1) {
            return list.get(0);
        }

        return list;
    }

    protected Object getFieldValue(Map<String, Object> document, String fieldName) {
        var rawValues = new ArrayList<>();
        processLevel(document, fieldName, rawValues);

        if (rawValues.size() == 1) {
            return rawValues.get(0);
        }

        return rawValues.stream().flatMap(v -> v instanceof List<?> l ? l.stream() : Stream.of(v)).toList();
    }

    @SuppressWarnings("unchecked")
    private void processLevel(Map<String, Object> level, String field, ArrayList<Object> values) {
        if (field.contains(".") == false) {
            if (level.containsKey(field)) {
                var value = level.get(field);
                values.add(value);
            }
            return;
        }

        var nameInLevel = field.split("\\.")[0];
        var entry = level.get(nameInLevel);
        if (entry instanceof Map<?, ?> m) {
            processLevel((Map<String, Object>) m, field.substring(field.indexOf('.') + 1), values);
        }
        if (entry instanceof List<?> l) {
            for (var object : l) {
                processLevel((Map<String, Object>) object, field.substring(field.indexOf('.') + 1), values);
            }
        }
    }

    public static boolean hasDocValues(Map<String, Object> fieldMapping, boolean defaultValue) {
        Object value = fieldMapping.getOrDefault("doc_values", defaultValue);
        if (value instanceof Boolean b) {
            return b;
        } else if (value instanceof Map) {
            return true;
        } else {
            throw new IllegalArgumentException("Unexpected value [" + value + "] for mapping parameter [doc_values]");
        }
    }
}
