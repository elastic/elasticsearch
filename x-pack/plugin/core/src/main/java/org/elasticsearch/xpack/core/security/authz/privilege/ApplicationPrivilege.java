/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
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

/**
 * An application privilege has a name (e.g. {@code "my-app:admin"}) that starts with a prefix that identifies the app,
 * followed by a suffix that is meaningful to the application.
 * It also has zero or more "action patterns" that start with the same prefix and contain at least 1 '/' character (e.g.
 * {@code "my-app:/admin/user/*", "my-app:/admin/team/*"}.
 * The action patterns are entirely optional - many application will find that simple "privilege names" are sufficient, but
 * they allow applications to define high level abstract privileges that map to multiple low level capabilities.
 */
public final class ApplicationPrivilege extends Privilege implements ToXContentObject, Writeable {

    public static final String DOC_TYPE_VALUE = "application-privilege";

    private static final ConstructingObjectParser<ApplicationPrivilege, Boolean> PARSER = new ConstructingObjectParser<>(DOC_TYPE_VALUE,
        arr -> new ApplicationPrivilege((String) arr[0], (String) arr[1], (List<String>) arr[2], (Map<String, Object>) arr[3])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Fields.APPLICATION);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Fields.NAME);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), Fields.ACTIONS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (parser, context) -> parser.map(), Fields.METADATA);
        PARSER.declareField((priv, fieldValue) -> {
            if (DOC_TYPE_VALUE.equals(fieldValue) == false) {
                throw new IllegalStateException("XContent has wrong " + Fields.TYPE.getPreferredName() + " field " + fieldValue);
            }
        }, (parser, acceptType) -> {
            if (acceptType == false) {
                throw new IllegalStateException("Field " + Fields.TYPE.getPreferredName() + " cannot be specified here");
            }
            return parser.text();
        }, Fields.TYPE, ObjectParser.ValueType.STRING);
    }

    private static final Pattern VALID_APPLICATION = Pattern.compile("^[a-z][A-Za-z0-9_-]{2,}$");
    private static final Pattern VALID_NAME = Pattern.compile("^[a-z][a-zA-Z0-9_.-]*$");

    public static final Function<String, ApplicationPrivilege> NONE = app -> new ApplicationPrivilege(app, "none", new String[0]);

    private final String application;
    private final String[] patterns;
    private final Map<String, Object> metadata;

    public ApplicationPrivilege(String application, String privilegeName, Collection<String> patterns, Map<String, Object> metadata) {
        this(application, Collections.singleton(privilegeName), patterns, metadata, true);
    }

    public ApplicationPrivilege(String application, String privilegeName, String... patterns) {
        this(application, Sets.newHashSet(privilegeName), patterns, Collections.emptyMap(), true);
    }

    private ApplicationPrivilege(String application, Set<String> name, Collection<String> patterns, Map<String, Object> metadata,
                                 boolean validateNames) {
        this(application, name, patterns.toArray(new String[patterns.size()]), metadata, validateNames);
    }

    private ApplicationPrivilege(String application, Set<String> name, String[] patterns, Map<String, Object> metadata,
                                 boolean validateNames) {
        super(name, patterns);
        this.application = application;
        this.patterns = patterns;
        this.metadata = new HashMap<>(metadata == null ? Collections.emptyMap() : metadata);
        validate(validateNames);
    }

    public String getApplication() {
        return application;
    }

    public Map<String, Object> getMetadata() {
        return Collections.unmodifiableMap(metadata);
    }

    private void validate(boolean validateNames) {
        if (VALID_APPLICATION.matcher(application).matches() == false) {
            throw new IllegalArgumentException("Application names must match /"
                + VALID_APPLICATION.pattern() + "/ (but was '" + application + "')");
        }

        for (String name : super.name()) {
            if (validateNames && isPrivilegeName(name) == false) {
                throw new IllegalArgumentException("Application privilege names must match the pattern /" + VALID_NAME.pattern()
                    + "/ (found '" + name + "')");
            }
        }
        for (String pattern : patterns) {
            if (pattern.indexOf('/') == -1 && pattern.indexOf('*') == -1 && pattern.indexOf(':') == -1) {
                throw new IllegalArgumentException(
                    "The application privilege pattern [" + pattern + "] must contain one of [ '/' , '*' , ':' ]");
            }
        }
    }

    public static boolean isPrivilegeName(String name) {
        return VALID_NAME.matcher(name).matches();
    }

    public static ApplicationPrivilege get(String application, Set<String> name, Collection<ApplicationPrivilege> stored) {
        if (name.isEmpty()) {
            return NONE.apply(application);
        } else {
            Map<String, ApplicationPrivilege> lookup = stored.stream()
                .filter(cp -> cp.application.equals(application))
                .filter(cp -> cp.name.size() == 1)
                .collect(Collectors.toMap(cp -> Iterables.get(cp.name, 0), Function.identity()));
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
            if (isPrivilegeName(name) != false) {
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
            return new ApplicationPrivilege(application, names, patterns, Collections.emptyMap(), true);
        } else {
            patterns.addAll(actions);
            return new ApplicationPrivilege(application, names, patterns, Collections.emptyMap(), false);
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
        return result;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && Objects.equals(this.application, ((ApplicationPrivilege) o).application);
    }

    public XContentBuilder toIndexContent(XContentBuilder builder, Params params) throws IOException {
        return writeXContent(builder, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return writeXContent(builder, false);
    }

    private XContentBuilder writeXContent(XContentBuilder builder, boolean includeType) throws IOException {
        assert name.size() == 1;

        builder.startObject()
            .field(Fields.APPLICATION.getPreferredName(), application)
            .field(Fields.NAME.getPreferredName(), Iterables.get(name, 0))
            .array(Fields.ACTIONS.getPreferredName(), this.patterns)
            .field(Fields.METADATA.getPreferredName(), this.metadata);

        if (includeType) {
            builder.field(Fields.TYPE.getPreferredName(), DOC_TYPE_VALUE);
        }
        return builder.endObject();
    }

    public static ApplicationPrivilege parse(XContentParser parser, boolean allowType) throws IOException {
        return PARSER.parse(parser, allowType);
    }

    public static ApplicationPrivilege readFrom(StreamInput in) throws IOException {
        final String application = in.readString();
        int nameSize = in.readVInt();
        Set<String> names = new HashSet<>(nameSize);
        for (int i = 0; i < nameSize; i++) {
            names.add(in.readString());
        }
        String[] patterns = in.readStringArray();
        Map<String, Object> metadata = in.readBoolean() ? in.readMap() : Collections.emptyMap();
        return new ApplicationPrivilege(application, names, patterns, metadata, false);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(application);
        out.writeVInt(name.size());
        for (String n : name) {
            out.writeString(n);
        }
        out.writeStringArray(patterns);
        if (metadata.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(metadata);
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
