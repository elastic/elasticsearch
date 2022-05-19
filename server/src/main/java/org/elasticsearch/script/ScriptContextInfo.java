/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ScriptContextInfo implements ToXContentObject, Writeable {
    public final String name;
    public final ScriptMethodInfo execute;
    public final Set<ScriptMethodInfo> getters;

    private static final String NAME_FIELD = "name";
    private static final String METHODS_FIELD = "methods";

    // ScriptService constructor
    ScriptContextInfo(String name, Class<?> clazz) {
        this.name = name;
        this.execute = ScriptMethodInfo.executeFromContext(clazz);
        this.getters = Collections.unmodifiableSet(ScriptMethodInfo.gettersFromContext(clazz));
    }

    // Deserialization constructor
    ScriptContextInfo(String name, List<ScriptMethodInfo> methods) {
        this.name = Objects.requireNonNull(name);
        Objects.requireNonNull(methods);

        String executeName = "execute";
        String getName = "get";
        // ignored instead of error, so future implementations can add methods. Same as ScriptContextInfo(String, Class).
        String otherName = "other";
        Map<String, List<ScriptMethodInfo>> methodTypes = methods.stream().collect(Collectors.groupingBy(m -> {
            if (m.name.equals(executeName)) {
                return executeName;
            } else if (m.name.startsWith(getName) && m.parameters.size() == 0) {
                return getName;
            }
            return otherName;
        }));

        if (methodTypes.containsKey(executeName) == false) {
            throw new IllegalArgumentException(
                "Could not find required method ["
                    + executeName
                    + "] in ["
                    + name
                    + "], found "
                    + methods.stream().map(m -> m.name).sorted().collect(Collectors.joining(", ", "[", "]"))
            );
        } else if ((methodTypes.get(executeName).size() != 1)) {
            throw new IllegalArgumentException(
                "Cannot have multiple [execute] methods in [" + name + "], found [" + methodTypes.get(executeName).size() + "]"
            );
        }
        this.execute = methodTypes.get(executeName).get(0);

        if (methodTypes.containsKey(getName)) {
            this.getters = Set.copyOf(methodTypes.get(getName));
        } else {
            this.getters = Collections.emptySet();
        }
    }

    // Test constructor
    public ScriptContextInfo(String name, ScriptMethodInfo execute, Set<ScriptMethodInfo> getters) {
        this.name = Objects.requireNonNull(name);
        this.execute = Objects.requireNonNull(execute);
        this.getters = Objects.requireNonNull(getters);
    }

    public ScriptContextInfo(StreamInput in) throws IOException {
        this.name = in.readString();
        this.execute = new ScriptMethodInfo(in);
        int numGetters = in.readInt();
        Set<ScriptMethodInfo> getters = new HashSet<>(numGetters);
        for (int i = 0; i < numGetters; i++) {
            getters.add(new ScriptMethodInfo(in));
        }
        this.getters = Collections.unmodifiableSet(getters);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        execute.writeTo(out);
        out.writeInt(getters.size());
        for (ScriptMethodInfo getter : getters) {
            getter.writeTo(out);
        }
    }

    public String getName() {
        return this.name;
    }

    public List<ScriptMethodInfo> methods() {
        ArrayList<ScriptMethodInfo> methods = new ArrayList<>();
        methods.add(this.execute);
        methods.addAll(this.getters);
        return Collections.unmodifiableList(methods);
    }

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ScriptContextInfo, Void> PARSER = new ConstructingObjectParser<>(
        "script_context_info",
        true,
        (m, name) -> new ScriptContextInfo((String) m[0], (List<ScriptMethodInfo>) m[1])
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField(NAME_FIELD));
        PARSER.declareObjectArray(
            constructorArg(),
            (parser, ctx) -> ScriptMethodInfo.PARSER.apply(parser, ctx),
            new ParseField(METHODS_FIELD)
        );
    }

    public static ScriptContextInfo fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScriptContextInfo that = (ScriptContextInfo) o;
        return Objects.equals(name, that.name) && Objects.equals(execute, that.execute) && Objects.equals(getters, that.getters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, execute, getters);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field(NAME_FIELD, name).startArray(METHODS_FIELD);
        execute.toXContent(builder, params);
        for (ScriptMethodInfo method : getters.stream().sorted(Comparator.comparing(g -> g.name)).toList()) {
            method.toXContent(builder, params);
        }
        return builder.endArray().endObject();
    }

    public static class ScriptMethodInfo implements ToXContentObject, Writeable {
        public final String name, returnType;
        public final List<ParameterInfo> parameters;

        static final String RETURN_TYPE_FIELD = "return_type";
        static final String PARAMETERS_FIELD = "params";

        public ScriptMethodInfo(String name, String returnType, List<ParameterInfo> parameters) {
            this.name = Objects.requireNonNull(name);
            this.returnType = Objects.requireNonNull(returnType);
            this.parameters = Collections.unmodifiableList(Objects.requireNonNull(parameters));
        }

        public ScriptMethodInfo(StreamInput in) throws IOException {
            this.name = in.readString();
            this.returnType = in.readString();
            int numParameters = in.readInt();
            ArrayList<ParameterInfo> parameters = new ArrayList<>(numParameters);
            for (int i = 0; i < numParameters; i++) {
                parameters.add(new ParameterInfo(in));
            }
            this.parameters = Collections.unmodifiableList(parameters);
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(returnType);
            out.writeInt(parameters.size());
            for (ParameterInfo parameter : parameters) {
                parameter.writeTo(out);
            }
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<ScriptMethodInfo, Void> PARSER = new ConstructingObjectParser<>(
            "method",
            true,
            (m, name) -> new ScriptMethodInfo((String) m[0], (String) m[1], (List<ParameterInfo>) m[2])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField(NAME_FIELD));
            PARSER.declareString(constructorArg(), new ParseField(RETURN_TYPE_FIELD));
            PARSER.declareObjectArray(
                constructorArg(),
                (parser, ctx) -> ParameterInfo.PARSER.apply(parser, ctx),
                new ParseField(PARAMETERS_FIELD)
            );
        }

        public static ScriptMethodInfo fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ScriptMethodInfo that = (ScriptMethodInfo) o;
            return Objects.equals(name, that.name)
                && Objects.equals(returnType, that.returnType)
                && Objects.equals(parameters, that.parameters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, returnType, parameters);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject().field(NAME_FIELD, name).field(RETURN_TYPE_FIELD, returnType).startArray(PARAMETERS_FIELD);
            for (ParameterInfo parameter : parameters) {
                parameter.toXContent(builder, params);
            }
            return builder.endArray().endObject();
        }

        public static class ParameterInfo implements ToXContentObject, Writeable {
            public final String type, name;

            public static final String TYPE_FIELD = "type";

            public ParameterInfo(String type, String name) {
                this.type = Objects.requireNonNull(type);
                this.name = Objects.requireNonNull(name);
            }

            public ParameterInfo(StreamInput in) throws IOException {
                this.type = in.readString();
                this.name = in.readString();
            }

            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(type);
                out.writeString(name);
            }

            private static final ConstructingObjectParser<ParameterInfo, Void> PARSER = new ConstructingObjectParser<>(
                "parameters",
                true,
                (p) -> new ParameterInfo((String) p[0], (String) p[1])
            );

            static {
                PARSER.declareString(constructorArg(), new ParseField(TYPE_FIELD));
                PARSER.declareString(constructorArg(), new ParseField(NAME_FIELD));
            }

            public static ParameterInfo fromXContent(XContentParser parser) throws IOException {
                return PARSER.parse(parser, null);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject().field(TYPE_FIELD, this.type).field(NAME_FIELD, this.name).endObject();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                ParameterInfo that = (ParameterInfo) o;
                return Objects.equals(type, that.type) && Objects.equals(name, that.name);
            }

            @Override
            public int hashCode() {
                return Objects.hash(type, name);
            }
        }

        static ScriptMethodInfo executeFromContext(Class<?> clazz) {
            Method execute = null;
            String name = "execute";

            // See ScriptContext.findMethod
            for (Method method : clazz.getMethods()) {
                if (method.getName().equals(name)) {
                    if (execute != null) {
                        throw new IllegalArgumentException(
                            "Cannot have multiple [" + name + "] methods on class [" + clazz.getName() + "]"
                        );
                    }
                    execute = method;
                }
            }
            if (execute == null) {
                throw new IllegalArgumentException("Could not find required method [" + name + "] on class [" + clazz.getName() + "]");
            }

            Class<?> returnTypeClazz = execute.getReturnType();
            String returnType = returnTypeClazz.getTypeName();

            Class<?>[] parameterTypes = execute.getParameterTypes();
            List<ParameterInfo> parameters = new ArrayList<>();
            if (parameterTypes.length > 0) {
                // TODO: ensure empty/no PARAMETERS if parameterTypes.length == 0?
                String parametersFieldName = "PARAMETERS";

                // See ScriptClassInfo.readArgumentNamesConstant
                Field parameterNamesField;
                try {
                    parameterNamesField = clazz.getField(parametersFieldName);
                } catch (NoSuchFieldException e) {
                    throw new IllegalArgumentException(
                        "Could not find field ["
                            + parametersFieldName
                            + "] on instance class ["
                            + clazz.getName()
                            + "] but method ["
                            + name
                            + "] has ["
                            + parameterTypes.length
                            + "] parameters"
                    );
                }
                if (parameterNamesField.getType().equals(String[].class) == false) {
                    throw new IllegalArgumentException(
                        "Expected a constant [String[] PARAMETERS] on instance class ["
                            + clazz.getName()
                            + "] for method ["
                            + name
                            + "] with ["
                            + parameterTypes.length
                            + "] parameters, found ["
                            + parameterNamesField.getType().getTypeName()
                            + "]"
                    );
                }

                String[] argumentNames;
                try {
                    argumentNames = (String[]) parameterNamesField.get(null);
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    throw new IllegalArgumentException("Error trying to read [" + clazz.getName() + "#ARGUMENTS]", e);
                }

                if (argumentNames.length != parameterTypes.length) {
                    throw new IllegalArgumentException(
                        "Expected argument names ["
                            + argumentNames.length
                            + "] to have the same arity ["
                            + parameterTypes.length
                            + "] for method ["
                            + name
                            + "] of class ["
                            + clazz.getName()
                            + "]"
                    );
                }

                for (int i = 0; i < argumentNames.length; i++) {
                    parameters.add(new ParameterInfo(parameterTypes[i].getTypeName(), argumentNames[i]));
                }
            }
            return new ScriptMethodInfo(name, returnType, parameters);
        }

        static Set<ScriptMethodInfo> gettersFromContext(Class<?> clazz) {
            // See ScriptClassInfo(PainlessLookup painlessLookup, Class<?> baseClass)
            HashSet<ScriptMethodInfo> getters = new HashSet<>();
            for (java.lang.reflect.Method m : clazz.getMethods()) {
                if (m.isDefault() == false
                    && m.getName().startsWith("get")
                    && m.getName().equals("getClass") == false
                    && Modifier.isStatic(m.getModifiers()) == false
                    && m.getParameters().length == 0) {
                    getters.add(new ScriptMethodInfo(m.getName(), m.getReturnType().getTypeName(), new ArrayList<>()));
                }
            }
            return getters;
        }
    }
}
