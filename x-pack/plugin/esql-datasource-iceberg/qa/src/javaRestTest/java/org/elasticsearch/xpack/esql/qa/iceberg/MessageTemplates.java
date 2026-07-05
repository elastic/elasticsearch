/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.iceberg;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple message template engine for loading and rendering messages from a template file.
 * Supports variable substitution using {{variable_name}} syntax and conditional blocks.
 * <p>
 * Output goes to a logger at WARN level to ensure visibility in test output.
 */
public class MessageTemplates {

    private static final Logger logger = LogManager.getLogger(MessageTemplates.class);

    private final Map<String, String> templates = new HashMap<>();
    private final Map<String, String> variables = new HashMap<>();
    private final PrintStream out;

    /**
     * Load templates from a resource file.
     * Uses System.err for output to ensure visibility (bypasses test output capture).
     *
     * @param resourcePath path to the template file
     * @return MessageTemplates instance
     * @throws IOException if the file cannot be read
     */
    public static MessageTemplates load(String resourcePath) throws IOException {
        MessageTemplates templates = new MessageTemplates(stderr());
        templates.loadFromResource(resourcePath);
        return templates;
    }

    /**
     * Create a MessageTemplates instance with custom output stream.
     *
     * @param out the output stream to use for printing
     */
    public MessageTemplates(PrintStream out) {
        this.out = out;
    }

    /**
     * Create a MessageTemplates instance using System.err.
     */
    public MessageTemplates() {
        this(stderr());
    }

    /**
     * Set a variable value for template substitution.
     *
     * @param name variable name
     * @param value variable value
     * @return this instance for chaining
     */
    public MessageTemplates set(String name, String value) {
        variables.put(name, value);
        return this;
    }

    /**
     * Set a variable value for template substitution.
     *
     * @param name variable name
     * @param value variable value (converted to string)
     * @return this instance for chaining
     */
    public MessageTemplates set(String name, long value) {
        return set(name, String.valueOf(value));
    }

    /**
     * Set a variable value for template substitution.
     *
     * @param name variable name
     * @param value variable value (converted to string)
     * @return this instance for chaining
     */
    public MessageTemplates set(String name, int value) {
        return set(name, String.valueOf(value));
    }

    /**
     * Get a rendered template by name.
     *
     * @param name template name (from [section] in the file)
     * @return rendered template with variables substituted
     */
    public String get(String name) {
        String template = templates.get(name);
        if (template == null) {
            return "[Template not found: " + name + "]";
        }
        return render(template);
    }

    /**
     * Print a template to the output stream.
     *
     * @param name template name
     */
    public void print(String name) {
        out.println(get(name));
    }

    /**
     * Print a formatted string to the output stream.
     *
     * @param format format string
     * @param args format arguments
     */
    public void printf(String format, Object... args) {
        out.printf(Locale.ROOT, format, args);
    }

    /**
     * Print a newline.
     */
    public void println() {
        out.println();
    }

    private void loadFromResource(String resourcePath) throws IOException {
        InputStream is = getClass().getResourceAsStream(resourcePath);
        if (is == null) {
            throw new IOException("Resource not found: " + resourcePath);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String currentSection = null;
            StringBuilder content = new StringBuilder();

            String line;
            while ((line = reader.readLine()) != null) {
                // Skip comments
                if (line.trim().startsWith("#")) {
                    continue;
                }

                // Check for section header [name]
                if (line.startsWith("[") && line.endsWith("]")) {
                    // Save previous section
                    if (currentSection != null) {
                        templates.put(currentSection, content.toString());
                    }

                    // Start new section
                    currentSection = line.substring(1, line.length() - 1);
                    content = new StringBuilder();
                } else if (currentSection != null) {
                    // Append to current section
                    content.append(line).append("\n");
                }
            }

            // Save last section
            if (currentSection != null) {
                templates.put(currentSection, content.toString());
            }
        }
    }

    private String render(String template) {
        String result = template;

        // Handle conditional blocks: {{#var}}content{{/var}}
        // Shows content only if variable exists and is not empty
        Pattern conditionalPattern = Pattern.compile("\\{\\{#(\\w+)\\}\\}([^{]*)\\{\\{/\\1\\}\\}");
        Matcher matcher = conditionalPattern.matcher(result);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String varName = matcher.group(1);
            String content = matcher.group(2);
            String value = variables.get(varName);
            String replacement = (value != null && value.isEmpty() == false) ? content : "";
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        result = sb.toString();

        // Replace simple variables: {{var}}
        for (Map.Entry<String, String> entry : variables.entrySet()) {
            String placeholder = "{{" + entry.getKey() + "}}";
            result = result.replace(placeholder, entry.getValue());
        }

        return result;
    }

    /**
     * Format bytes for display.
     */
    public static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
        } else {
            return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024.0));
        }
    }

    /**
     * Format time as MM:SS.
     */
    public static String formatTime(long minutes, long seconds) {
        return String.format(Locale.ROOT, "%d:%02d", minutes, seconds);
    }

    @SuppressForbidden(reason = "System.err is intentional for this interactive manual testing tool")
    private static PrintStream stderr() {
        return System.err;
    }
}
