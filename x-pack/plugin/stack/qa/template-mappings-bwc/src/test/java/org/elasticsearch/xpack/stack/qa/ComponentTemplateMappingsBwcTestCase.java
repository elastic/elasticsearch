/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stack.qa;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Abstract base for backwards-compatibility tests on x-pack component-template mappings.
 *
 * <p>A subclass declares the classpath resource path and the map of template variable substitutions.
 * The test then:
 * <ol>
 *   <li>Loads the previous-branch mapping from the BWC git checkout directory (passed via the
 *       {@code tests.bwc.checkoutDir} system property set by the per-version Gradle task).</li>
 *   <li>Loads the current mapping from the classpath.</li>
 *   <li>Asserts that {@code MapperService.merge(MAPPING_UPDATE)} of current onto previous succeeds,
 *       catching type changes and other merge-rejected modifications.</li>
 *   <li>Asserts that every leaf field path in the previous mapping is still present in a fresh mapper
 *       service built from the current mapping alone, catching field removals.</li>
 * </ol>
 *
 * <p>The test skips gracefully ({@code assumeTrue}) when the resource does not exist on the previous
 * branch, which happens when the registry was introduced after that branch was cut.
 */
public abstract class ComponentTemplateMappingsBwcTestCase extends MapperServiceTestCase {

    /**
     * Classpath resource path to the component-template JSON, e.g.
     * {@code "/activitylog/logs-elasticsearch.querylog@mappings.json"}.
     * The resource must be on the classpath of both the current branch (via module dependencies)
     * and present on disk under the BWC checkout for previous branches.
     */
    protected abstract String mappingsResourcePath();

    /**
     * All {@code ${variable}} substitutions to apply to the template JSON before parsing, keyed by
     * variable name. Typically contains at least the template version, e.g.
     * {@code Map.of("xpack.stack.querylog.template.version", "4")}.
     */
    protected abstract Map<String, String> templateVariables();

    /**
     * Path inside the BWC checkout directory where the template-resources module's sources live.
     * Set by the Gradle task via {@code tests.bwc.templateResourcesDir}; override in a subclass
     * if the resource lives in a different module within the checkout.
     */
    protected String templateResourcesSourcePath() {
        return System.getProperty("tests.bwc.templateResourcesDir", "x-pack/plugin/core/template-resources/src/main/resources");
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new ConstantKeywordMapperPlugin());
    }

    public final void testCurrentMappingIsBackwardsCompatibleWithPreviousBranch() throws IOException {
        String bwcDir = System.getProperty("tests.bwc.checkoutDir");
        String bwcVersion = System.getProperty("tests.bwc.version", "unknown");
        assumeTrue("BWC checkout not configured (tests.bwc.checkoutDir not set)", bwcDir != null);

        Path previousFile = Path.of(bwcDir, templateResourcesSourcePath() + mappingsResourcePath());
        assumeTrue(
            "Mapping resource not present on previous branch " + bwcVersion + ": " + previousFile,
            Files.isRegularFile(previousFile)
        );

        String previousJson = substituteVariables(Files.readString(previousFile));
        String currentJson;
        try (InputStream in = getClass().getResourceAsStream(mappingsResourcePath())) {
            assertNotNull("Current mapping resource not found on classpath: " + mappingsResourcePath(), in);
            currentJson = substituteVariables(new String(in.readAllBytes(), UTF_8));
        }

        // Merging current onto previous catches type changes and other merge-rejected edits.
        MapperService previousService = createMapperService(extractMappingsSubtree(previousJson));
        merge(previousService, MapperService.MergeReason.MAPPING_UPDATE, extractMappingsSubtree(currentJson));

        // MapperService.merge is additive and never removes fields; build a fresh service from the
        // current mapping alone to detect field deletions.
        MapperService currentService = createMapperService(extractMappingsSubtree(currentJson));
        assertAllPreviousLeafPathsResolvable(currentService, previousJson, bwcVersion);
    }

    private String substituteVariables(String json) {
        return templateVariables().entrySet()
            .stream()
            .reduce(json, (s, e) -> TemplateUtils.replaceVariable(s, e.getKey(), e.getValue()), (a, b) -> b);
    }

    /**
     * Extracts the inner {@code template.mappings} block from the component-template JSON and wraps
     * it as {@code {"_doc": ...}} for {@link MapperServiceTestCase#createMapperService(String)}.
     */
    @SuppressWarnings("unchecked")
    private static String extractMappingsSubtree(String templateJson) throws IOException {
        Map<String, Object> root;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, templateJson)) {
            root = parser.map();
        }
        Map<String, Object> template = (Map<String, Object>) root.get("template");
        Map<String, Object> mappings = (Map<String, Object>) template.get("mappings");

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("_doc");
            for (Map.Entry<String, Object> entry : mappings.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            builder.endObject();
            return Strings.toString(builder);
        }
    }

    /**
     * Walks the {@code properties} tree in the previous-branch mapping and asserts that each leaf
     * field is still resolvable in {@code currentService}.
     * Dynamic object fields with no static sub-properties are skipped.
     */
    @SuppressWarnings("unchecked")
    private static void assertAllPreviousLeafPathsResolvable(MapperService currentService, String previousJson, String bwcVersion)
        throws IOException {
        Map<String, Object> root;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, previousJson)) {
            root = parser.map();
        }
        Map<String, Object> template = (Map<String, Object>) root.get("template");
        Map<String, Object> mappings = (Map<String, Object>) template.get("mappings");
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        if (properties != null) {
            walkProperties(currentService, "", properties, bwcVersion);
        }
    }

    @SuppressWarnings("unchecked")
    private static void walkProperties(MapperService currentService, String prefix, Map<String, Object> properties, String bwcVersion) {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String path = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Map<String, Object> fieldDef = (Map<String, Object>) entry.getValue();
            Map<String, Object> nested = (Map<String, Object>) fieldDef.get("properties");
            if (nested != null) {
                walkProperties(currentService, path, nested, bwcVersion);
            } else if ("object".equals(fieldDef.get("type")) == false && fieldDef.get("type") != null) {
                assertNotNull(
                    "Field [" + path + "] present in previous branch (" + bwcVersion + ") mapping is missing from the current mapping",
                    currentService.fieldType(path)
                );
            }
            // else: Dynamic object with no static sub-properties
        }
    }
}
