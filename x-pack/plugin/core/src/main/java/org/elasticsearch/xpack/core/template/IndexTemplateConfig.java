/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Describes an index template to be loaded from a resource file for use with an {@link IndexTemplateRegistry}.
 */
public class IndexTemplateConfig {

    private final String templateName;
    private final String fileName;
    private final int version;
    private final String versionProperty;
    private final Map<String, String> variables;

    /**
     * Describes a template to be loaded from a resource file. Includes handling for substituting a version property into the template.
     *
     * The {@code versionProperty} parameter will be used to substitute the value of {@code version} into the template. For example,
     * this template:
     * {@code {"myTemplateVersion": "${my.version.property}"}}
     * With {@code version = "42"; versionProperty = "my.version.property"} will result in {@code {"myTemplateVersion": "42"}}.
     *
     * Note that this code does not automatically insert the {@code version} index template property - include that in the JSON file
     * defining the template, preferably using the version variable provided to this constructor.
     *
     *  @param templateName The name that will be used for the index template. Literal, include the version in this string if
     *                     it should be used.
     * @param fileName The filename the template should be loaded from. Literal, should include leading {@literal /} and
     *                 extension if necessary.
     * @param version The version of the template. Substituted for {@code versionProperty} as described above.
     * @param versionProperty The property that will be replaced with the {@code version} string as described above.
     */
    public IndexTemplateConfig(String templateName, String fileName, int version, String versionProperty) {
        this(templateName, fileName, version, versionProperty, Collections.emptyMap());
    }

    /**
     * Describes a template to be loaded from a resource file. Includes handling for substituting a version property into the template.
     *
     * The {@code versionProperty} parameter will be used to substitute the value of {@code version} into the template. For example,
     * this template:
     * {@code {"myTemplateVersion": "${my.version.property}"}}
     * With {@code version = "42"; versionProperty = "my.version.property"} will result in {@code {"myTemplateVersion": "42"}}.
     *
     * @param templateName The name that will be used for the index template. Literal, include the version in this string if
     *                     it should be used.
     * @param fileName The filename the template should be loaded from. Literal, should include leading {@literal /} and
     *                 extension if necessary.
     * @param version The version of the template. Substituted for {@code versionProperty} as described above.
     * @param versionProperty The property that will be replaced with the {@code version} string as described above.
     * @param variables A map of additional variable substitutions. The map's keys are the variable names.
     *                 The corresponding values will replace the variable names.
     */
    public IndexTemplateConfig(String templateName, String fileName, int version, String versionProperty, Map<String, String> variables) {
        this.templateName = templateName;
        this.fileName = fileName;
        this.version = version;
        this.versionProperty = versionProperty;
        this.variables = Objects.requireNonNull(variables);
    }

    public String getFileName() {
        return fileName;
    }

    public String getTemplateName() {
        return templateName;
    }

    public int getVersion() {
        return version;
    }

    /**
     * Loads the template from disk as a UTF-8 byte array.
     * @return The template as a UTF-8 byte array.
     */
    public byte[] loadBytes() {
        String template = TemplateUtils.loadTemplate(fileName, Integer.toString(version), versionProperty, variables);
        assert template != null && template.length() > 0;
        assert Pattern.compile("\"version\"\\s*:\\s*" + version).matcher(template).find()
            : "index template must have a version property set to the given version property";
        return template.getBytes(StandardCharsets.UTF_8);
    }
}
