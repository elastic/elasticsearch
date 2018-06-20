/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * An application privilege has an application name (e.g. {@code "my-app"}) that identifies an application (that exists
 * outside of elasticsearch), a privilege name (e.g. {@code "admin}) that is meaningful to that application, and zero or
 * more "action patterns" (e.g {@code "admin/user/*", "admin/team/*"}).
 * Action patterns must contain at least one special character from ({@code /}, {@code :}, {@code *}) to distinguish them
 * from privilege names.
 * The action patterns are entirely optional - many application will find that simple "privilege names" are sufficient, but
 * they allow applications to define high level abstract privileges that map to multiple low level capabilities.
 */
public final class ApplicationPrivilege extends Privilege implements ToXContentObject, Writeable {

    public static final String DOC_TYPE_VALUE = "application-privilege";

    private static final ObjectParser<Builder, Boolean> PARSER = new ObjectParser<>(DOC_TYPE_VALUE, Builder::new);

    static {
        PARSER.declareString(Builder::applicationName, Fields.APPLICATION);
        PARSER.declareString(Builder::privilegeName, Fields.NAME);
        PARSER.declareStringArray(Builder::actions, Fields.ACTIONS);
        PARSER.declareObject(Builder::metadata, (parser, context) -> parser.map(), Fields.METADATA);
        PARSER.declareField((parser, builder, allowType) -> builder.type(parser.text(), allowType), Fields.TYPE,
            ObjectParser.ValueType.STRING);
    }

    private static final Pattern VALID_APPLICATION = Pattern.compile("^[a-z][A-Za-z0-9_-]{2,}$");
    private static final Pattern VALID_APPLICATION_OR_WILDCARD = Pattern.compile("^[A-Za-z0-9_*-]+");
    private static final Pattern VALID_NAME = Pattern.compile("^[a-z][a-zA-Z0-9_.-]*$");

    public static final Function<String, ApplicationPrivilege> NONE = app -> new ApplicationPrivilege(app, "none", new String[0]);

    private final String application;
    private final String[] patterns;
    private final Map<String, Object> metadata;

    public ApplicationPrivilege(String application, String privilegeName, Collection<String> patterns, Map<String, Object> metadata) {
        this(application, Collections.singleton(privilegeName), patterns.toArray(new String[patterns.size()]), metadata, true);
    }

    public ApplicationPrivilege(String application, String privilegeName, String... patterns) {
        this(application, Collections.singleton(privilegeName), patterns, emptyMap(), true);
    }

    private ApplicationPrivilege(String application, Set<String> name, String[] patterns, Map<String, Object> metadata,
                                 boolean validateNames) {
        super(name, patterns);
        this.application = application;
        this.patterns = patterns;
        this.metadata = new HashMap<>(metadata == null ? emptyMap() : metadata);
        validate(validateNames);
    }

    public String getApplication() {
        return application;
    }

    /**
     * If this privilege has a single name, returns that name. Otherwise throws {@link IllegalStateException}.
     *
     * @see #name()
     */
    public String getPrivilegeName() {
        if (name.size() == 1) {
            return name.iterator().next();
        } else {
            throw new IllegalStateException(this + " has a multivariate name: " + Strings.collectionToCommaDelimitedString(name));
        }
    }

    public Map<String, Object> getMetadata() {
        return Collections.unmodifiableMap(metadata);
    }

    // Package level for testing
    String[] getPatterns() {
        return patterns;
    }

    private void validate(boolean validateNames) {
        // Treat wildcards differently so that the error message matches the context
        if (Regex.isSimpleMatchPattern(application)) {
            validateApplicationName(application, VALID_APPLICATION_OR_WILDCARD);
        } else {
            validateApplicationName(application, VALID_APPLICATION);
        }

        for (String name : super.name()) {
            if (validateNames && isValidPrivilegeName(name) == false) {
                throw new IllegalArgumentException("Application privilege names must match the pattern " + VALID_NAME.pattern()
                    + " (found '" + name + "')");
            }
        }
        for (String pattern : patterns) {
            if (pattern.indexOf('/') == -1 && pattern.indexOf('*') == -1 && pattern.indexOf(':') == -1) {
                throw new IllegalArgumentException(
                    "The application privilege pattern [" + pattern + "] must contain one of [ '/' , '*' , ':' ]");
            }
        }
    }

    /**
     * Validate that the provided application name is valid, and throws an exception otherwise
     *
     * @throws IllegalArgumentException if the name is not valid
     */
    public static void validateApplicationName(String application) {
        validateApplicationName(application, VALID_APPLICATION);
    }

    private static void validateApplicationName(String application, Pattern pattern) {
        if (pattern.matcher(application).matches() == false) {
            throw new IllegalArgumentException("Application names must match the pattern " + pattern.pattern()
                + " (but was '" + application + "')");
        }
    }

    private static boolean isValidPrivilegeName(String name) {
        return VALID_NAME.matcher(name).matches();
    }

    /**
     * Finds or creates an application privileges with the provided names.
     * Each element in {@code name} may be the name of a stored privilege (to be resolved from {@code stored}, or a bespoke action pattern.
     */
    public static ApplicationPrivilege get(String application, Set<String> name, Collection<ApplicationPrivilege> stored) {
        if (name.isEmpty()) {
            return NONE.apply(application);
        } else {
            Map<String, ApplicationPrivilege> lookup = stored.stream()
                .filter(cp -> cp.application.equals(application))
                .filter(cp -> cp.name.size() == 1)
                .collect(Collectors.toMap(ApplicationPrivilege::getPrivilegeName, Function.identity()));
            return resolve(application, name, lookup);
        }
    }

    private static ApplicationPrivilege resolve(String application, Set<String> names, Map<String, ApplicationPrivilege> lookup) {
        final int size = names.size();
        if (size == 0) {
            throw new IllegalArgumentException("empty set should not be used");
        }

        Set<String> actions = new HashSet<>();
        Set<String> patterns = new HashSet<>();
        for (String name : names) {
            name = name.toLowerCase(Locale.ROOT);
            if (isValidPrivilegeName(name)) {
                ApplicationPrivilege privilege = lookup.get(name);
                if (privilege != null && size == 1) {
                    return privilege;
                } else if (privilege != null) {
                    patterns.addAll(Arrays.asList(privilege.patterns));
                } else {
                    throw new IllegalArgumentException("unknown application privilege [" + names + "]");
                }
            } else {
                actions.add(name);
            }
        }

        if (actions.isEmpty()) {
            return new ApplicationPrivilege(application, names, patterns.toArray(new String[patterns.size()]), emptyMap(), true);
        } else {
            patterns.addAll(actions);
            return new ApplicationPrivilege(application, names, patterns.toArray(new String[patterns.size()]), emptyMap(), false);
        }
    }

    @Override
    public String toString() {
        return application + ":" + super.toString() + "(" + Strings.arrayToCommaDelimitedString(patterns) + ")";
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hashCode(application);
        result = 31 * result + Arrays.hashCode(patterns);
        result = 31 * result + Objects.hashCode(metadata);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o)
            && Objects.equals(this.application, ((ApplicationPrivilege) o).application)
            && Arrays.equals(this.patterns, ((ApplicationPrivilege) o).patterns)
            && Objects.equals(this.metadata, ((ApplicationPrivilege) o).metadata);
    }

    /**
     * Converts this object to XContent suitable for storing in the security index - this includes the "type" parameter that is needed
     * for index-persistence, but is not used in the Rest API.
     */
    public XContentBuilder toIndexContent(XContentBuilder builder) throws IOException {
        return writeXContent(builder, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return writeXContent(builder, false);
    }

    private XContentBuilder writeXContent(XContentBuilder builder, boolean includeType) throws IOException {
        builder.startObject()
            .field(Fields.APPLICATION.getPreferredName(), application)
            .field(Fields.NAME.getPreferredName(), getPrivilegeName())
            .array(Fields.ACTIONS.getPreferredName(), this.patterns)
            .field(Fields.METADATA.getPreferredName(), this.metadata);

        if (includeType) {
            builder.field(Fields.TYPE.getPreferredName(), DOC_TYPE_VALUE);
        }
        return builder.endObject();
    }

    /**
     * Construct a new {@link ApplicationPrivilege} from XContent.
     *
     * @param allowType If true, accept a "type" field (for which the value must match {@link #DOC_TYPE_VALUE});
     */
    public static ApplicationPrivilege parse(XContentParser parser, String defaultApplication, String defaultPrivilegeName,
                                             boolean allowType) throws IOException {
        final Builder builder = PARSER.parse(parser, allowType);
        if (builder.applicationName == null) {
            builder.applicationName(defaultApplication);
        }
        if (builder.privilegeName == null) {
            builder.privilegeName(defaultPrivilegeName);
        }
        return builder.build();
    }

    public static ApplicationPrivilege readFrom(StreamInput in) throws IOException {
        final String application = in.readString();
        Set<String> names = in.readSet(StreamInput::readString);
        String[] patterns = in.readStringArray();
        Map<String, Object> metadata = (Map<String, Object>) in.readGenericValue();
        return new ApplicationPrivilege(application, names, patterns, metadata, false);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(application);
        out.writeCollection(name, StreamOutput::writeString);
        out.writeStringArray(patterns);
        out.writeGenericValue(metadata);
    }

    private static final class Builder {
        private String applicationName;
        private String privilegeName;
        private List<String> actions;
        private Map<String, Object> metadata;

        private Builder applicationName(String applicationName) {
            this.applicationName = applicationName;
            return this;
        }

        private Builder privilegeName(String privilegeName) {
            this.privilegeName = privilegeName;
            return this;
        }

        private Builder actions(List<String> actions) {
            this.actions = actions;
            return this;
        }

        private Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        private Builder type(String type, boolean allowed) {
            if (allowed == false) {
                throw new IllegalStateException("Field " + Fields.TYPE.getPreferredName() + " cannot be specified here");
            }
            if (DOC_TYPE_VALUE.equals(type) == false) {
                throw new IllegalStateException("XContent has wrong " + Fields.TYPE.getPreferredName() + " field " + type);
            }
            return this;
        }

        private ApplicationPrivilege build() {
            return new ApplicationPrivilege(applicationName, privilegeName, actions, metadata);
        }
    }

    public interface Fields {
        ParseField APPLICATION = new ParseField("application");
        ParseField NAME = new ParseField("name");
        ParseField ACTIONS = new ParseField("actions");
        ParseField METADATA = new ParseField("metadata");
        ParseField TYPE = new ParseField("type");
    }

}
