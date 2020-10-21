/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.security.support.MustacheTemplateEvaluator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A model for a service provider (see {@link SamlServiceProvider} and {@link SamlServiceProviderDocument}) that uses wildcard matching
 * rules and a service-provider template.
 */
class WildcardServiceProvider {

    private static final ConstructingObjectParser<WildcardServiceProvider, Void> PARSER = new ConstructingObjectParser<>(
        "wildcard_service",
        args -> {
            final String entityId = (String) args[0];
            final String acs = (String) args[1];
            final Collection<String> tokens = (Collection<String>) args[2];
            final Map<String, Object> definition = (Map<String, Object>) args[3];
            return new WildcardServiceProvider(entityId, acs, tokens, definition);
        });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Fields.ENTITY_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Fields.ACS);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), Fields.TOKENS);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, ignore) -> p.map(), Fields.TEMPLATE);
    }

    private final Pattern matchEntityId;
    private final Pattern matchAcs;
    private final Set<String> tokens;
    private final BytesReference serviceTemplate;

    private WildcardServiceProvider(Pattern matchEntityId, Pattern matchAcs, Set<String> tokens, BytesReference serviceTemplate) {
        this.matchEntityId = Objects.requireNonNull(matchEntityId);
        this.matchAcs = Objects.requireNonNull(matchAcs);
        this.tokens = Objects.requireNonNull(tokens);
        this.serviceTemplate = Objects.requireNonNull(serviceTemplate);
    }

    WildcardServiceProvider(String matchEntityId, String matchAcs, Collection<String> tokens, Map<String, Object> serviceTemplate) {
        this(Pattern.compile(Objects.requireNonNull(matchEntityId, "EntityID to match cannot be null")),
            Pattern.compile(Objects.requireNonNull(matchAcs, "ACS to match cannot be null")),
            Set.copyOf(Objects.requireNonNull(tokens, "Tokens collection may not be null")),
            toMustacheScript(Objects.requireNonNull(serviceTemplate, "Service definition may not be null")));
    }

    public static WildcardServiceProvider parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final WildcardServiceProvider that = (WildcardServiceProvider) o;
        return matchEntityId.pattern().equals(that.matchEntityId.pattern()) &&
            matchAcs.pattern().equals(that.matchAcs.pattern()) &&
            tokens.equals(that.tokens) &&
            serviceTemplate.equals(that.serviceTemplate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(matchEntityId.pattern(), matchAcs.pattern(), tokens, serviceTemplate);
    }

    private static BytesReference toMustacheScript(Map<String, Object> serviceDefinition) {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            builder.field("source");
            builder.map(serviceDefinition);
            builder.endObject();
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Nullable
    public SamlServiceProviderDocument apply(ScriptService scriptService, final String entityId, final String acs) {
        Map<String, Object> parameters = extractTokens(entityId, acs);
        if (parameters == null) {
            return null;
        }
        try {
            String serviceJson = evaluateTemplate(scriptService, parameters);
            final SamlServiceProviderDocument doc = toServiceProviderDocument(serviceJson);
            final Instant now = Instant.now();
            doc.setEntityId(entityId);
            doc.setAcs(acs);
            doc.setCreated(now);
            doc.setLastModified(now);
            return doc;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // package protected for testing
    Map<String, Object> extractTokens(String entityId, String acs) {
        final Matcher entityIdMatcher = this.matchEntityId.matcher(entityId);
        if (entityIdMatcher.matches() == false) {
            return null;
        }
        final Matcher acsMatcher = this.matchAcs.matcher(acs);
        if (acsMatcher.matches() == false) {
            return null;
        }

        Map<String, Object> parameters = new HashMap<>();
        for (String token : this.tokens) {
            String entityIdToken = extractGroup(entityIdMatcher, token);
            String acsToken = extractGroup(acsMatcher, token);
            if (entityIdToken != null) {
                if (acsToken != null) {
                    if (entityIdToken.equals(acsToken) == false) {
                        throw new IllegalArgumentException("Extracted token [" + token + "] values from EntityID ([" + entityIdToken
                            + "] from [" + entityId + "]) and ACS ([" + acsToken + "] from [" + acs + "]) do not match");
                    }
                }
                parameters.put(token, entityIdToken);
            } else if (acsToken != null) {
                parameters.put(token, acsToken);
            }
        }
        parameters.putIfAbsent("entity_id", entityId);
        parameters.putIfAbsent("acs", acs);
        return parameters;
    }

    private String evaluateTemplate(ScriptService scriptService, Map<String, Object> parameters) throws IOException {
        try (XContentParser templateParser = parser(serviceTemplate)) {
            return MustacheTemplateEvaluator.evaluate(scriptService, templateParser, parameters);
        }
    }

    private SamlServiceProviderDocument toServiceProviderDocument(String serviceJson) throws IOException {
        try (XContentParser docParser = parser(new BytesArray(serviceJson))) {
            return SamlServiceProviderDocument.fromXContent(null, docParser);
        }
    }

    private static XContentParser parser(BytesReference body) throws IOException {
        return XContentHelper.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, body, XContentType.JSON);
    }

    private String extractGroup(Matcher matcher, String name) {
        try {
            return matcher.group(name);
        } catch (IllegalArgumentException e) {
            // Stoopid java API, ignore
            return null;
        }
    }

    public interface Fields {
        ParseField ENTITY_ID = new ParseField("entity_id");
        ParseField ACS = new ParseField("acs");
        ParseField TOKENS = new ParseField("tokens");
        ParseField TEMPLATE = new ParseField("template");
    }

}
