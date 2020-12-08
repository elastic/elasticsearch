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

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.painless.action.PainlessContextClassInfo;
import org.elasticsearch.painless.action.PainlessContextConstructorInfo;
import org.elasticsearch.painless.action.PainlessContextFieldInfo;
import org.elasticsearch.painless.action.PainlessContextMethodInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PainlessInfoJson {
    public static class Class implements ToXContentObject {
        private final String name;
        private final boolean imported;
        private final List<PainlessContextConstructorInfo> constructors;
        private final List<Method> staticMethods;
        private final List<Method> methods;
        private final List<PainlessContextFieldInfo> staticFields;
        private final List<PainlessContextFieldInfo> fields;

        public Class(PainlessContextClassInfo info, Map<String, String> javaNamesToDisplayNames) {
            this.name = info.getName();
            this.imported = info.isImported();
            this.constructors = info.getConstructors();
            this.staticMethods = Method.fromMethodInfos(info.getStaticMethods(), javaNamesToDisplayNames);
            this.methods = Method.fromMethodInfos(info.getMethods(), javaNamesToDisplayNames);
            this.staticFields = info.getStaticFields();
            this.fields = info.getFields();
        }

        public static List<Class> fromClassInfos(List<PainlessContextClassInfo> infos, Map<String, String> javaNamesToDisplayNames) {
            return infos.stream()
                .map(info -> new Class(info, javaNamesToDisplayNames))
                .collect(Collectors.toList());
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
        private final List<String> parameters;

        public Method(PainlessContextMethodInfo info, Map<String, String> javaNamesToDisplayNames) {
            this.declaring = info.getDeclaring();
            this.name = info.getName();
            this.rtn = ContextGeneratorCommon.getType(javaNamesToDisplayNames, info.getRtn());
            this.parameters = info.getParameters().stream()
                .map(p -> ContextGeneratorCommon.getType(javaNamesToDisplayNames, p))
                .collect(Collectors.toList());
        }

        public static List<Method> fromMethodInfos(List<PainlessContextMethodInfo> infos, Map<String, String> javaNamesToDisplayNames) {
            return infos.stream()
                .map(m -> new Method(m, javaNamesToDisplayNames))
                .collect(Collectors.toList());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(PainlessContextMethodInfo.DECLARING.getPreferredName(), declaring);
            builder.field(PainlessContextMethodInfo.NAME.getPreferredName(), name);
            builder.field(PainlessContextMethodInfo.RTN.getPreferredName(), rtn);
            builder.field(PainlessContextMethodInfo.PARAMETERS.getPreferredName(), parameters);
            builder.endObject();

            return builder;
        }
    }
}
