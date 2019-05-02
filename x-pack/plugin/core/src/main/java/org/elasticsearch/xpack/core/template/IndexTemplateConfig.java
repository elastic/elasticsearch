/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.template;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Describes an index template to be loaded from a resource file for use with an {@link IndexTemplateRegistry}.
 */
public class IndexTemplateConfig {

    private final String templateName;
    private final String fileName;
    private final String version;
    private final String versionProperty;

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
     */
    public IndexTemplateConfig(String templateName, String fileName, String version, String versionProperty) {
        this.templateName = templateName;
        this.fileName = fileName;
        this.version = version;
        this.versionProperty = versionProperty;
    }

    public String getFileName() {
        return fileName;
    }

    public String getTemplateName() {
        return templateName;
    }

    /**
     * Loads the template from disk as a UTF-8 byte array.
     * @return The template as a UTF-8 byte array.
     */
    public byte[] loadBytes() {
        String template = TemplateUtils.loadTemplate(fileName, version,
            Pattern.quote("${" + versionProperty + "}"));
        assert template != null && template.length() > 0;
        return template.getBytes(StandardCharsets.UTF_8);
    }
}
