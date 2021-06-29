/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.owasp.html.CssSchema;
import org.owasp.html.ElementPolicy;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class HtmlSanitizer {

    static final String[] FORMATTING_TAGS = new String[] {
            "b", "i", "s", "u", "o", "sup", "sub", "ins", "del", "strong",
            "strike", "tt", "code", "big", "small", "br", "span", "em", "hr"
    };
    static final String[] BLOCK_TAGS = new String[] {
            "p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "ul", "ol", "li", "blockquote"
    };
    static final String[] TABLE_TAGS = new String[] {
            "table", "th", "tr", "td", "caption", "col", "colgroup", "thead", "tbody", "tfoot"
    };
    static final List<String> DEFAULT_ALLOWED = Arrays.asList(
            "body", "head", "_tables", "_links", "_blocks", "_formatting", "img:embedded"
    );

    private static Setting<Boolean> SETTING_SANITIZATION_ENABLED =
            Setting.boolSetting("xpack.notification.email.html.sanitization.enabled", true, Property.NodeScope);

    private static Setting<List<String>> SETTING_SANITIZATION_ALLOW =
            Setting.listSetting("xpack.notification.email.html.sanitization.allow", DEFAULT_ALLOWED, Function.identity(),
                    Property.NodeScope);

    private static Setting<List<String>> SETTING_SANITIZATION_DISALLOW =
            Setting.listSetting("xpack.notification.email.html.sanitization.disallow", Collections.emptyList(), Function.identity(),
                    Property.NodeScope);
    private static final MethodHandle sanitizeHandle;
    static {
        try {
            MethodHandles.Lookup methodLookup = MethodHandles.publicLookup();
            MethodType sanitizeSignature = MethodType.methodType(String.class, String.class);
            sanitizeHandle = methodLookup.findVirtual(PolicyFactory.class, "sanitize", sanitizeSignature);
        } catch (NoSuchMethodException|IllegalAccessException e) {
            throw new RuntimeException("Missing guava on runtime classpath", e);
        }
    }

    private final boolean enabled;
    private final UnaryOperator<String> sanitizer;

    public HtmlSanitizer(Settings settings) {
        enabled = SETTING_SANITIZATION_ENABLED.get(settings);
        List<String> allow = SETTING_SANITIZATION_ALLOW.get(settings);
        List<String> disallow = SETTING_SANITIZATION_DISALLOW.get(settings);

        // The sanitize method of PolicyFactory pulls in guava dependencies, which we want to isolate to
        // runtime only rather than compile time where more guava uses can be accidentally pulled in.
        // Here we lookup the sanitize method at runtime and grab a method handle to invoke.
        PolicyFactory policy = createCommonPolicy(allow, disallow);
        sanitizer = s -> {
            try {
                return (String) sanitizeHandle.invokeExact(policy, s);
            } catch (Throwable e) {
                throw new RuntimeException("Failed to invoke sanitize method of PolicyFactory", e);
            }
        };
    }

    public String sanitize(String html) {
        if (enabled == false) {
            return html;
        }
        return sanitizer.apply(html);
    }

    @SuppressForbidden( reason = "PolicyFactory uses guava Function")
    static PolicyFactory createCommonPolicy(List<String> allow, List<String> disallow) {
        HtmlPolicyBuilder policyBuilder = new HtmlPolicyBuilder();

        if (allow.stream().anyMatch("_all"::equals)) {
            return policyBuilder
                    .allowElements(TABLE_TAGS)
                    .allowAttributes("span").onElements("col")
                    .allowElements(BLOCK_TAGS)
                    .allowElements(FORMATTING_TAGS)
                    .allowWithoutAttributes("span")
                    .allowStyling(CssSchema.DEFAULT)
                    .allowStandardUrlProtocols().allowElements("a")
                    .allowAttributes("href").onElements("a").requireRelNofollowOnLinks()
                    .allowElements("img")
                    .allowAttributes("src").onElements("img")
                    .allowStandardUrlProtocols()
                    .allowUrlProtocols("cid")
                    .toFactory();
        }

        EnumSet<Images> images = EnumSet.noneOf(Images.class);

        for (String tag : allow) {
            tag = tag.toLowerCase(Locale.ROOT);
            switch (tag) {
                case "_tables":
                    policyBuilder.allowElements(TABLE_TAGS);
                    policyBuilder.allowAttributes("span").onElements("col");
                    policyBuilder.allowAttributes("border", "cellpadding").onElements("table");
                    policyBuilder.allowAttributes("colspan", "rowspan").onElements("th", "td");
                    break;
                case "_links":
                    policyBuilder.allowElements("a")
                            .allowAttributes("href").onElements("a")
                            .allowStandardUrlProtocols()
                            .requireRelNofollowOnLinks();
                    break;
                case "_blocks":
                    policyBuilder.allowElements(BLOCK_TAGS);
                    break;
                case "_formatting":
                    policyBuilder.allowElements(FORMATTING_TAGS)
                            .allowWithoutAttributes("span");
                    break;
                case "_styles":
                    policyBuilder.allowStyling(CssSchema.DEFAULT);
                    break;
                case "img:all":
                case "img":
                    images.add(Images.ALL);
                    break;
                case "img:embedded":
                    images.add(Images.EMBEDDED);
                    break;
                default:
                    policyBuilder.allowElements(tag);
            }
        }
        for (String tag : disallow) {
            tag = tag.toLowerCase(Locale.ROOT);
            switch (tag) {
                case "_tables":
                    policyBuilder.disallowElements(TABLE_TAGS);
                    break;
                case "_links":
                    policyBuilder.disallowElements("a");
                    break;
                case "_blocks":
                    policyBuilder.disallowElements(BLOCK_TAGS);
                    break;
                case "_formatting":
                    policyBuilder.disallowElements(FORMATTING_TAGS);
                    break;
                case "_styles":
                    policyBuilder.disallowAttributes("style");
                    break;
                case "img:all":
                case "img":
                    images.remove(Images.ALL);
                    break;
                case "img:embedded":
                    images.remove(Images.EMBEDDED);
                    break;
                default:
                    policyBuilder.disallowElements(tag);
            }
        }

        if (images.isEmpty() == false) {
            policyBuilder.allowAttributes("src").onElements("img").allowUrlProtocols("cid");
            if (images.contains(Images.ALL)) {
                policyBuilder.allowElements("img");
                policyBuilder.allowStandardUrlProtocols();
            } else {
                // embedded
                policyBuilder.allowElements(EmbeddedImgOnlyPolicy.INSTANCE, "img");
            }
        }

        return policyBuilder.toFactory();
    }



    /**
     * An {@code img} tag policy that only accept {@code cid:} values in its {@code src} attribute.
     * If such value is found, the content id is verified against the available attachements of the
     * email and if the content/attachment is not found, the element is dropped.
     */
    private static class EmbeddedImgOnlyPolicy implements ElementPolicy {

        private static EmbeddedImgOnlyPolicy INSTANCE = new EmbeddedImgOnlyPolicy();

        @Override
        public String apply(String elementName, List<String> attrs) {
            if ("img".equals(elementName) == false || attrs.isEmpty()) {
                return elementName;
            }
            String attrName = null;
            for (String attr : attrs) {
                if (attrName == null) {
                    attrName = attr.toLowerCase(Locale.ROOT);
                    continue;
                }
                // reject external image source (only allow embedded ones)
                if ("src".equals(attrName) && attr.startsWith("cid:") == false) {
                    return null;
                }
            }
            return elementName;
        }
    }

    enum Images {
        ALL,
        EMBEDDED
    }

    public static List<Setting<?>> getSettings() {
        return Arrays.asList(SETTING_SANITIZATION_ALLOW, SETTING_SANITIZATION_DISALLOW, SETTING_SANITIZATION_ENABLED);
    }
}
