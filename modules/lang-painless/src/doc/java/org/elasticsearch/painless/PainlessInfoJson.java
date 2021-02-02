/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.painless.action.PainlessContextClassBindingInfo;
import org.elasticsearch.painless.action.PainlessContextClassInfo;
import org.elasticsearch.painless.action.PainlessContextConstructorInfo;
import org.elasticsearch.painless.action.PainlessContextFieldInfo;
import org.elasticsearch.painless.action.PainlessContextInfo;
import org.elasticsearch.painless.action.PainlessContextInstanceBindingInfo;
import org.elasticsearch.painless.action.PainlessContextMethodInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PainlessInfoJson {
    public static class Context implements ToXContentObject {
        private final String name;
        private final List<Class> classes;
        private final List<Method> importedMethods;
        private final List<PainlessContextClassBindingInfo> classBindings;
        private final List<PainlessContextInstanceBindingInfo> instanceBindings;

        public Context(
            PainlessContextInfo info,
            Set<PainlessContextClassInfo> commonClassInfos,
            Map<String, String> javaNamesToDisplayNames,
            StdlibJavadocExtractor extractor
        ) throws IOException {
            this.name = info.getName();
            List<PainlessContextClassInfo> classInfos = ContextGeneratorCommon.excludeCommonClassInfos(commonClassInfos, info.getClasses());
            classInfos = ContextGeneratorCommon.sortClassInfos(classInfos);
            this.classes = Class.fromInfos(classInfos, javaNamesToDisplayNames, extractor);
            this.importedMethods = Method.fromInfos(
                    info.getImportedMethods(),
                    javaNamesToDisplayNames,
                    new StdlibJavadocExtractor.ParsedJavaClass()
            );
            this.classBindings = info.getClassBindings();
            this.instanceBindings = info.getInstanceBindings();
        }

        public Context(
            PainlessContextInfo info,
            Set<PainlessContextClassInfo> commonClassInfos,
            Map<String, String> javaNamesToDisplayNames
        ) {
            this.name = info.getName();
            List<PainlessContextClassInfo> classInfos = ContextGeneratorCommon.excludeCommonClassInfos(commonClassInfos, info.getClasses());
            classInfos = ContextGeneratorCommon.sortClassInfos(classInfos);
            this.classes = Class.fromInfos(classInfos, javaNamesToDisplayNames);
            this.importedMethods = Method.fromInfos(
                    info.getImportedMethods(),
                    javaNamesToDisplayNames,
                    new StdlibJavadocExtractor.ParsedJavaClass()
            );
            this.classBindings = info.getClassBindings();
            this.instanceBindings = info.getInstanceBindings();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PainlessContextInfo.NAME.getPreferredName(), name);
            builder.field(PainlessContextInfo.CLASSES.getPreferredName(), classes);
            builder.field(PainlessContextInfo.IMPORTED_METHODS.getPreferredName(), importedMethods);
            builder.field(PainlessContextInfo.CLASS_BINDINGS.getPreferredName(), classBindings);
            builder.field(PainlessContextInfo.INSTANCE_BINDINGS.getPreferredName(), instanceBindings);
            builder.endObject();

            return builder;
        }

        public String getName() {
            return name;
        }
    }

    public static class Class implements ToXContentObject {
        private final String name;
        private final boolean imported;
        private final List<Constructor> constructors;
        private final List<Method> staticMethods;
        private final List<Method> methods;
        private final List<Field> staticFields;
        private final List<Field> fields;

        public Class(
            PainlessContextClassInfo info,
            Map<String, String> javaNamesToDisplayNames,
            StdlibJavadocExtractor.ParsedJavaClass pj
        ) {
            this.name = javaNamesToDisplayNames.get(info.getName());
            this.imported = info.isImported();
            this.constructors = Constructor.fromInfos(info.getConstructors(), javaNamesToDisplayNames, pj);
            this.staticMethods = Method.fromInfos(info.getStaticMethods(), javaNamesToDisplayNames, pj);
            this.methods = Method.fromInfos(info.getMethods(), javaNamesToDisplayNames, pj);
            this.staticFields = Field.fromInfos(info.getStaticFields(), javaNamesToDisplayNames, pj);
            this.fields = Field.fromInfos(info.getFields(), javaNamesToDisplayNames, pj);
        }

        public static List<Class> fromInfos(
            List<PainlessContextClassInfo> infos,
            Map<String, String> javaNamesToDisplayNames,
            StdlibJavadocExtractor extractor
        ) throws IOException {
            List<Class> classes = new ArrayList<>(infos.size());
            for (PainlessContextClassInfo info : infos) {
                StdlibJavadocExtractor.ParsedJavaClass parsedClass = extractor.parseClass(info.getName());
                classes.add(new Class(info, javaNamesToDisplayNames, parsedClass));
            }
            return classes;
        }

        public static List<Class> fromInfos(
            List<PainlessContextClassInfo> infos,
            Map<String, String> javaNamesToDisplayNames
        ) {
            List<Class> classes = new ArrayList<>(infos.size());
            for (PainlessContextClassInfo info : infos) {
                classes.add(new Class(info, javaNamesToDisplayNames, null));
            }
            return classes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PainlessContextClassInfo.NAME.getPreferredName(), name);
            builder.field(PainlessContextClassInfo.IMPORTED.getPreferredName(), imported);
            builder.field(PainlessContextClassInfo.CONSTRUCTORS.getPreferredName(), constructors);
            builder.field(PainlessContextClassInfo.STATIC_METHODS.getPreferredName(), staticMethods);
            builder.field(PainlessContextClassInfo.METHODS.getPreferredName(), methods);
            builder.field(PainlessContextClassInfo.STATIC_FIELDS.getPreferredName(), staticFields);
            builder.field(PainlessContextClassInfo.FIELDS.getPreferredName(), fields);
            builder.endObject();

            return builder;
        }
    }

    public static class Method implements ToXContentObject {
        private final String declaring;
        private final String name;
        private final String rtn;
        private final String javadoc;
        private final List<String> parameters;
        private final List<String> parameterNames;
        public static final ParseField PARAMETER_NAMES = new ParseField("parameter_names");
        public static final ParseField JAVADOC = new ParseField("javadoc");

        public Method(
                PainlessContextMethodInfo info,
                Map<String, String> javaNamesToDisplayNames,
                String javadoc,
                List<String> parameterNames
        ) {
            this.declaring = javaNamesToDisplayNames.get(info.getDeclaring());
            this.name = info.getName();
            this.rtn = ContextGeneratorCommon.getType(javaNamesToDisplayNames, info.getRtn());
            this.javadoc = javadoc;
            this.parameters = info.getParameters().stream()
                    .map(p -> ContextGeneratorCommon.getType(javaNamesToDisplayNames, p))
                    .collect(Collectors.toList());
            this.parameterNames = parameterNames;
        }

        public static List<Method> fromInfos(
                List<PainlessContextMethodInfo> infos,
                Map<String, String> javaNamesToDisplayNames,
                StdlibJavadocExtractor.ParsedJavaClass parsed
        ) {
            List<Method> methods = new ArrayList<>(infos.size());
            for (PainlessContextMethodInfo info: infos) {
                StdlibJavadocExtractor.ParsedMethod parsedMethod = parsed != null ? parsed.getMethod(info, javaNamesToDisplayNames) : null;
                if (parsedMethod == null) {
                    parsedMethod = new StdlibJavadocExtractor.ParsedMethod("", Collections.emptyList());
                }
                methods.add(new Method(info, javaNamesToDisplayNames, parsedMethod.javadoc, parsedMethod.parameterNames));
            }
            return methods;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PainlessContextMethodInfo.DECLARING.getPreferredName(), declaring);
            builder.field(PainlessContextMethodInfo.NAME.getPreferredName(), name);
            builder.field(PainlessContextMethodInfo.RTN.getPreferredName(), rtn);
            if (javadoc != null && "".equals(javadoc) == false) {
                builder.field(JAVADOC.getPreferredName(), javadoc);
            }
            builder.field(PainlessContextMethodInfo.PARAMETERS.getPreferredName(), parameters);
            if (parameterNames != null && parameterNames.size() > 0) {
                builder.field(PARAMETER_NAMES.getPreferredName(), parameterNames);
            }
            builder.endObject();

            return builder;
        }
    }

    public static class Constructor implements ToXContentObject {
        private final String declaring;
        private final List<String> parameters;
        private final List<String> parameterNames;
        private final String javadoc;

        public static final ParseField JAVADOC = new ParseField("javadoc");
        public static final ParseField PARAMETER_NAMES = new ParseField("parameter_names");

        public Constructor(
                PainlessContextConstructorInfo info,
                Map<String, String> javaNamesToDisplayNames,
                StdlibJavadocExtractor.ParsedJavaClass pj
        ) {
            this.declaring = javaNamesToDisplayNames.get(info.getDeclaring());
            this.parameters = info.getParameters().stream()
                .map(p -> ContextGeneratorCommon.getType(javaNamesToDisplayNames, p))
                .collect(Collectors.toList());
            StdlibJavadocExtractor.ParsedMethod parsed = pj != null ? pj.getConstructor(parameters) : null;
            if (parsed == null) {
                this.parameterNames = null;
                this.javadoc = null;
            } else {
                this.parameterNames = parsed.parameterNames;
                this.javadoc = parsed.javadoc;
            }
        }

        public static List<Constructor> fromInfos(
            List<PainlessContextConstructorInfo> infos,
            Map<String, String> javaNamesToDisplayNames,
            StdlibJavadocExtractor.ParsedJavaClass pj
        ) {
            return infos.stream()
                .map(c -> new Constructor(c, javaNamesToDisplayNames, pj))
                .collect(Collectors.toList());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PainlessContextConstructorInfo.DECLARING.getPreferredName(), declaring);
            builder.field(PainlessContextConstructorInfo.PARAMETERS.getPreferredName(), parameters);
            if (parameterNames != null && parameterNames.size() > 0) {
                builder.field(PARAMETER_NAMES.getPreferredName(), parameterNames);
            }
            if (javadoc != null && "".equals(javadoc) == false) {
                builder.field(JAVADOC.getPreferredName(), javadoc);
            }
            builder.endObject();

            return builder;
        }
    }

    public static class Field implements ToXContentObject {
        private final String declaring;
        private final String name;
        private final String type;
        private final String javadoc;

        public static final ParseField JAVADOC = new ParseField("javadoc");

        public Field(
                PainlessContextFieldInfo info,
                Map<String, String> javaNamesToDisplayNames,
                StdlibJavadocExtractor.ParsedJavaClass pj
        ) {
            this.declaring = javaNamesToDisplayNames.get(info.getDeclaring());
            this.name = info.getName();
            this.type = ContextGeneratorCommon.getType(javaNamesToDisplayNames, info.getType());
            this.javadoc = pj != null ? pj.getField(name) : null;
        }

        public static List<Field> fromInfos(
            List<PainlessContextFieldInfo> infos,
            Map<String, String> javaNamesToDisplayNames,
            StdlibJavadocExtractor.ParsedJavaClass pj
        ) {
            return infos.stream()
                    .map(f -> new Field(f, javaNamesToDisplayNames, pj))
                    .collect(Collectors.toList());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PainlessContextFieldInfo.DECLARING.getPreferredName(), declaring);
            builder.field(PainlessContextFieldInfo.NAME.getPreferredName(), name);
            builder.field(PainlessContextFieldInfo.TYPE.getPreferredName(), type);
            if (javadoc != null && "".equals(javadoc) == false) {
                builder.field(JAVADOC.getPreferredName(), javadoc);
            }
            builder.endObject();

            return builder;
        }
    }
}
