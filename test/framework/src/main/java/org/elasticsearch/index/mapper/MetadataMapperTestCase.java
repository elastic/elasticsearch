/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public abstract class MetadataMapperTestCase extends MapperServiceTestCase {

    protected abstract String fieldName();

    protected abstract boolean isConfigurable();

    protected boolean isSupportedOn(IndexVersion version) {
        return true;
    }

    protected abstract void registerParameters(ParameterChecker checker) throws IOException;

    private record ConflictCheck(XContentBuilder init, XContentBuilder update, Consumer<DocumentMapper> check) {}

    private record UpdateCheck(String paramName, XContentBuilder init, XContentBuilder update, Consumer<DocumentMapper> check) {}

    public class ParameterChecker {

        Map<String, ConflictCheck> conflictChecks = new HashMap<>();
        List<UpdateCheck> updateChecks = new ArrayList<>();
        Set<String> checkedParameters = new HashSet<>();
        Set<String> serializationExclusions = new HashSet<>();

        /**
         * Register a check that a parameter update will cause a conflict, using the minimal mapping as a base
         *
         * @param param  the parameter name, expected to appear in the error message
         * @param update a field builder applied on top of the minimal mapping
         */
        public void registerConflictCheck(String param, CheckedConsumer<XContentBuilder, IOException> update) throws IOException {
            checkedParameters.add(param);
            conflictChecks.put(param, new ConflictCheck(topMapping(b -> b.startObject(fieldName()).endObject()), topMapping(b -> {
                b.startObject(fieldName());
                update.accept(b);
                b.endObject();
            }), d -> {}));
        }

        /**
         * Register a check that a parameter update will cause a conflict
         *
         * @param param  the parameter name, expected to appear in the error message
         * @param init   the initial mapping
         * @param update the updated mapping
         */
        public void registerConflictCheck(String param, XContentBuilder init, XContentBuilder update, Consumer<DocumentMapper> check) {
            checkedParameters.add(param);
            conflictChecks.put(param, new ConflictCheck(init, update, check));
        }

        /**
         * Register a check that a parameter can be updated
         *
         * @param param  the parameter name
         * @param init   the initial mapping
         * @param update the updated mapping
         * @param check  a check that the updated parameter has been applied to the DocumentMapper
         */
        public void registerUpdateCheck(String param, XContentBuilder init, XContentBuilder update, Consumer<DocumentMapper> check) {
            checkedParameters.add(param);
            updateChecks.add(new UpdateCheck(param, init, update, check));
        }

        /**
         * Register a parameter returned from getParameters() that does not need an update or conflict check,
         * for example a parameter that is tested separately.
         * @param param the parameter name
         */
        public void registerIgnoredParameter(String param) {
            checkedParameters.add(param);
        }

        /**
         * Exclude a parameter from the serialization test, for example when
         * the parameter is intentionally not serialized in the mapping.
         * @param param the parameter name
         */
        public void excludeFromSerialization(String param) {
            serializationExclusions.add(param);
        }

        /**
         * Asserts that every parameter returned by the given builder's {@link MetadataFieldMapper.Builder#getParameters()}
         * has been registered with either an update check or a conflict check.
         */
        public void ensureAllParametersAreCovered(MetadataFieldMapper.Builder builder) {
            Set<String> uncovered = Arrays.stream(builder.getParameters()).map(p -> p.name).collect(Collectors.toSet());
            uncovered.removeAll(checkedParameters);
            assertTrue("Some parameters are not covered by either an update check or a conflict check: " + uncovered, uncovered.isEmpty());
        }
    }

    public final void testUpdates() throws IOException {
        assumeTrue("Metadata field " + fieldName() + " isn't configurable", isConfigurable());
        ParameterChecker checker = new ParameterChecker();
        registerParameters(checker);
        for (String param : checker.conflictChecks.keySet()) {
            MapperService mapperService = createMapperService(checker.conflictChecks.get(param).init);
            // merging the same change is fine
            merge(mapperService, checker.conflictChecks.get(param).init);
            // merging the conflicting update should throw an exception
            Exception e = expectThrows(
                IllegalArgumentException.class,
                "No conflict when updating parameter [" + param + "]",
                () -> merge(mapperService, checker.conflictChecks.get(param).update)
            );
            assertThat(
                e.getMessage(),
                anyOf(containsString("Cannot update parameter [" + param + "]"), containsString("different [" + param + "]"))
            );
            checker.conflictChecks.get(param).check.accept(mapperService.documentMapper());
        }
        for (UpdateCheck updateCheck : checker.updateChecks) {
            MapperService mapperService = createMapperService(updateCheck.init);
            // merging is fine
            merge(mapperService, updateCheck.update);
            // run the update assertion
            updateCheck.check.accept(mapperService.documentMapper());
        }
    }

    public void testParameterSerialization() throws IOException {
        assumeTrue("Metadata field " + fieldName() + " isn't configurable", isConfigurable());
        ParameterChecker checker = new ParameterChecker();
        registerParameters(checker);

        for (UpdateCheck updateCheck : checker.updateChecks) {
            if (checker.serializationExclusions.contains(updateCheck.paramName)) {
                continue;
            }
            String initSerialized = serializeMapping(createMapperService(updateCheck.init));
            String updateSerialized = serializeMapping(createMapperService(updateCheck.update));
            assertTrue(
                "serialized mapping for update check on ["
                    + updateCheck.paramName
                    + "] should contain the parameter name"
                    + " in either init or update mapping. Init: "
                    + initSerialized
                    + "; Update: "
                    + updateSerialized,
                initSerialized.contains(updateCheck.paramName) || updateSerialized.contains(updateCheck.paramName)
            );
        }

        for (Map.Entry<String, ConflictCheck> entry : checker.conflictChecks.entrySet()) {
            String param = entry.getKey();
            if (checker.serializationExclusions.contains(param)) {
                continue;
            }
            ConflictCheck conflictCheck = entry.getValue();
            String initSerialized = serializeMapping(createMapperService(conflictCheck.init));
            String updateSerialized = serializeMapping(createMapperService(conflictCheck.update));
            assertTrue(
                "serialized mapping for conflict check on ["
                    + param
                    + "] should contain the parameter name"
                    + " in either init or update mapping. Init: "
                    + initSerialized
                    + "; Update: "
                    + updateSerialized,
                initSerialized.contains(param) || updateSerialized.contains(param)
            );
        }
    }

    private static String serializeMapping(MapperService mapperService) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        mapperService.documentMapper().mapping().toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return Strings.toString(builder);
    }

    public void testAllParametersAreChecked() throws IOException {

        ParameterChecker checker = new ParameterChecker();
        registerParameters(checker);

        if (isConfigurable() == false) {
            assertThat(checker.checkedParameters, hasSize(0));
        }

        MappingBuilder mappings = parseMappings(topMapping(b -> {}));
        MetadataFieldMapper.Builder builder = mappings.metadataBuilder(fieldName());
        checker.ensureAllParametersAreCovered(builder);
    }

    public final void testUnsupportedParametersAreRejected() throws IOException {
        assumeTrue("Metadata field " + fieldName() + " isn't configurable", isConfigurable());
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion();
        assumeTrue("Metadata field " + fieldName() + " is not supported on version " + version, isSupportedOn(version));
        MapperService mapperService = createMapperService(version, mapping(xContentBuilder -> {}));
        String mappingAsString = "{\n"
            + "    \"_doc\" : {\n"
            + "      \""
            + fieldName()
            + "\" : {\n"
            + "        \"anything\" : \"anything\"\n"
            + "      }\n"
            + "    }\n"
            + "}";
        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> mapperService.parseMapping("_doc", MergeReason.MAPPING_UPDATE, new CompressedXContent(mappingAsString))
        );
        assertEquals(
            "Failed to parse mapping: unknown parameter [anything] on metadata field [" + fieldName() + "]",
            exception.getMessage()
        );
    }

    public final void testFixedMetaFieldsAreNotConfigurable() throws IOException {
        assumeFalse("Metadata field " + fieldName() + " is configurable", isConfigurable());
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion();
        assumeTrue("Metadata field " + fieldName() + " is not supported on version " + version, isSupportedOn(version));
        MapperService mapperService = createMapperService(version, mapping(xContentBuilder -> {}));
        String mappingAsString = "{\n" + "    \"_doc\" : {\n" + "      \"" + fieldName() + "\" : {\n" + "      }\n" + "    }\n" + "}";
        MapperParsingException exception = expectThrows(
            MapperParsingException.class,
            () -> mapperService.parseMapping("_doc", MergeReason.MAPPING_UPDATE, new CompressedXContent(mappingAsString))
        );
        assertEquals("Failed to parse mapping: " + fieldName() + " is not configurable", exception.getMessage());
    }

    public void testTypeAndFriendsAreAcceptedBefore_8_6_0() throws IOException {
        assumeTrue("Metadata field " + fieldName() + " isn't configurable", isConfigurable());
        IndexVersion previousVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.V_8_6_0);
        // we randomly also pick read-only versions to test that we can still parse the parameters for them
        IndexVersion version = IndexVersionUtils.randomVersionBetween(IndexVersionUtils.getLowestReadCompatibleVersion(), previousVersion);
        assumeTrue("Metadata field " + fieldName() + " is not supported on version " + version, isSupportedOn(version));
        MapperService mapperService = createMapperService(version, mapping(b -> {}));
        // these parameters were previously silently ignored, they will still be ignored in existing indices
        String[] unsupportedParameters = new String[] { "fields", "copy_to", "boost", "type" };
        for (String param : unsupportedParameters) {
            String mappingAsString = "{\n"
                + "    \"_doc\" : {\n"
                + "      \""
                + fieldName()
                + "\" : {\n"
                + "        \""
                + param
                + "\" : \"any\"\n"
                + "      }\n"
                + "    }\n"
                + "}";
            assertNotNull(mapperService.parseMapping("_doc", MergeReason.MAPPING_UPDATE, new CompressedXContent(mappingAsString)));
        }
    }

    public void testTypeAndFriendsAreDeprecatedFrom_8_6_0_TO_9_0_0() throws IOException {
        assumeTrue("Metadata field " + fieldName() + " isn't configurable", isConfigurable());
        IndexVersion previousVersion = IndexVersionUtils.getPreviousVersion(IndexVersions.UPGRADE_TO_LUCENE_10_0_0);
        IndexVersion version = IndexVersionUtils.randomVersionBetween(IndexVersions.V_8_6_0, previousVersion);
        assumeTrue("Metadata field " + fieldName() + " is not supported on version " + version, isSupportedOn(version));
        MapperService mapperService = createMapperService(version, mapping(b -> {}));
        // these parameters were deprecated, they now should throw an error in new indices
        String[] unsupportedParameters = new String[] { "fields", "copy_to", "boost", "type" };
        for (String param : unsupportedParameters) {
            String mappingAsString = "{\n"
                + "    \"_doc\" : {\n"
                + "      \""
                + fieldName()
                + "\" : {\n"
                + "        \""
                + param
                + "\" : \"any\"\n"
                + "      }\n"
                + "    }\n"
                + "}";
            assertNotNull(mapperService.parseMapping("_doc", MergeReason.MAPPING_UPDATE, new CompressedXContent(mappingAsString)));
            assertWarnings("Parameter [" + param + "] has no effect on metadata field [" + fieldName() + "] and will be removed in future");
        }
    }

    public void testTypeAndFriendsThrow_After_9_0_0() throws IOException {
        assumeTrue("Metadata field " + fieldName() + " isn't configurable", isConfigurable());
        IndexVersion version = IndexVersionUtils.randomVersionBetween(IndexVersions.UPGRADE_TO_LUCENE_10_0_0, IndexVersion.current());
        assumeTrue("Metadata field " + fieldName() + " is not supported on version " + version, isSupportedOn(version));
        MapperService mapperService = createMapperService(version, mapping(b -> {}));
        // these parameters were previously silently ignored, they are now deprecated in new indices
        String[] unsupportedParameters = new String[] { "fields", "copy_to", "boost", "type" };
        for (String param : unsupportedParameters) {
            String mappingAsString = "{\n"
                + "    \"_doc\" : {\n"
                + "      \""
                + fieldName()
                + "\" : {\n"
                + "        \""
                + param
                + "\" : \"any\"\n"
                + "      }\n"
                + "    }\n"
                + "}";
            expectThrows(
                MapperParsingException.class,
                () -> mapperService.parseMapping("_doc", MergeReason.MAPPING_UPDATE, new CompressedXContent(mappingAsString))
            );
        }
    }
}
