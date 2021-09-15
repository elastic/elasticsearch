/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.painless.action.PainlessContextClassBindingInfo;
import org.elasticsearch.painless.action.PainlessContextClassInfo;
import org.elasticsearch.painless.action.PainlessContextInfo;
import org.elasticsearch.painless.action.PainlessContextInstanceBindingInfo;
import org.elasticsearch.painless.action.PainlessContextMethodInfo;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ContextGeneratorCommon {
    @SuppressForbidden(reason = "retrieving data from an internal API not exposed as part of the REST client")
    public static List<PainlessContextInfo> getContextInfos() throws IOException {
        URLConnection getContextNames = new URL(
            "http://" + System.getProperty("cluster.uri") + "/_scripts/painless/_context").openConnection();
        XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, getContextNames.getInputStream());
        parser.nextToken();
        parser.nextToken();
        @SuppressWarnings("unchecked")
        List<String> contextNames = (List<String>)(Object)parser.list();
        parser.close();
        ((HttpURLConnection)getContextNames).disconnect();

        List<PainlessContextInfo> contextInfos = new ArrayList<>();

        for (String contextName : contextNames) {
            URLConnection getContextInfo = new URL(
                "http://" + System.getProperty("cluster.uri") + "/_scripts/painless/_context?context=" + contextName).openConnection();
            parser = JsonXContent.jsonXContent.createParser(null, null, getContextInfo.getInputStream());
            contextInfos.add(PainlessContextInfo.fromXContent(parser));
            ((HttpURLConnection)getContextInfo).disconnect();
        }

        contextInfos.sort(Comparator.comparing(PainlessContextInfo::getName));

        return contextInfos;
    }

    public static String getType(Map<String, String> javaNamesToDisplayNames, String javaType) {
        if (javaType.endsWith("[]") == false) {
            return javaNamesToDisplayNames.getOrDefault(javaType, javaType);
        }
        int bracePosition = javaType.indexOf('[');
        String braces = javaType.substring(bracePosition);
        String type = javaType.substring(0, bracePosition);
        if (javaNamesToDisplayNames.containsKey(type)) {
            return javaNamesToDisplayNames.get(type) + braces;
        }
        return javaType;
    }

    private static Map<String, String> getDisplayNames(Collection<PainlessContextInfo> contextInfos) {
        Map<String, String> javaNamesToDisplayNames = new HashMap<>();

        for (PainlessContextInfo contextInfo : contextInfos) {
            for (PainlessContextClassInfo classInfo : contextInfo.getClasses()) {
                String className = classInfo.getName();
                if (javaNamesToDisplayNames.containsKey(className) == false) {
                    if (classInfo.isImported()) {
                        javaNamesToDisplayNames.put(className,
                            className.substring(className.lastIndexOf('.') + 1).replace('$', '.'));
                    } else {
                        javaNamesToDisplayNames.put(className, className.replace('$', '.'));
                    }
                }
            }
        }
        return javaNamesToDisplayNames;
    }

    public static List<PainlessContextClassInfo> sortClassInfos(Collection<PainlessContextClassInfo> unsortedClassInfos) {

        List<PainlessContextClassInfo> classInfos = new ArrayList<>(unsortedClassInfos);
        classInfos.removeIf(v ->
            "void".equals(v.getName())  || "boolean".equals(v.getName()) || "byte".equals(v.getName())   ||
                "short".equals(v.getName()) || "char".equals(v.getName())    || "int".equals(v.getName())    ||
                "long".equals(v.getName())  || "float".equals(v.getName())   || "double".equals(v.getName()) ||
                "org.elasticsearch.painless.lookup.def".equals(v.getName())  ||
                isInternalClass(v.getName())
        );

        classInfos.sort((c1, c2) -> {
            String n1 = c1.getName();
            String n2 = c2.getName();
            boolean i1 = c1.isImported();
            boolean i2 = c2.isImported();

            String p1 = n1.substring(0, n1.lastIndexOf('.'));
            String p2 = n2.substring(0, n2.lastIndexOf('.'));

            int compare = p1.compareTo(p2);

            if (compare == 0) {
                if (i1 && i2) {
                    compare = n1.substring(n1.lastIndexOf('.') + 1).compareTo(n2.substring(n2.lastIndexOf('.') + 1));
                } else if (i1 == false && i2 == false) {
                    compare = n1.compareTo(n2);
                } else {
                    compare = Boolean.compare(i1, i2) * -1;
                }
            }

            return compare;
        });

        return classInfos;
    }

    private static boolean isInternalClass(String javaName) {
        return  javaName.equals("org.elasticsearch.script.ScoreScript") ||
            javaName.equals("org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoShape") ||
            javaName.equals("org.elasticsearch.xpack.sql.expression.function.scalar.whitelist.InternalSqlScriptUtils") ||
            javaName.equals("org.elasticsearch.xpack.sql.expression.literal.IntervalDayTime") ||
            javaName.equals("org.elasticsearch.xpack.sql.expression.literal.IntervalYearMonth") ||
            javaName.equals("org.elasticsearch.xpack.eql.expression.function.scalar.whitelist.InternalEqlScriptUtils") ||
            javaName.equals("org.elasticsearch.xpack.ql.expression.function.scalar.InternalQlScriptUtils") ||
            javaName.equals("org.elasticsearch.xpack.ql.expression.function.scalar.whitelist.InternalQlScriptUtils") ||
            javaName.equals("org.elasticsearch.script.ScoreScript$ExplanationHolder");
    }

    public static List<PainlessContextClassInfo> excludeCommonClassInfos(
        Set<PainlessContextClassInfo> exclude,
        List<PainlessContextClassInfo> classInfos
    ) {
        List<PainlessContextClassInfo> uniqueClassInfos = new ArrayList<>(classInfos);
        uniqueClassInfos.removeIf(exclude::contains);
        return uniqueClassInfos;
    }

    public static class PainlessInfos {
        public final Set<PainlessContextMethodInfo> importedMethods;
        public final Set<PainlessContextClassBindingInfo> classBindings;
        public final Set<PainlessContextInstanceBindingInfo> instanceBindings;

        public final List<PainlessInfoJson.Class> common;
        public final List<PainlessInfoJson.Context> contexts;

        public final Map<String, String> javaNamesToDisplayNames;
        public final Map<String, String> javaNamesToJavadoc;
        public final Map<String, List<String>> javaNamesToArgs;

        public PainlessInfos(List<PainlessContextInfo> contextInfos) {
            javaNamesToDisplayNames = getDisplayNames(contextInfos);

            javaNamesToJavadoc = new HashMap<>();
            javaNamesToArgs = new HashMap<>();

            Set<PainlessContextClassInfo> commonClassInfos = getCommon(contextInfos, PainlessContextInfo::getClasses);
            common = PainlessInfoJson.Class.fromInfos(sortClassInfos(commonClassInfos), javaNamesToDisplayNames);

            importedMethods = getCommon(contextInfos, PainlessContextInfo::getImportedMethods);

            classBindings = getCommon(contextInfos, PainlessContextInfo::getClassBindings);

            instanceBindings = getCommon(contextInfos, PainlessContextInfo::getInstanceBindings);

            contexts = contextInfos.stream()
                .map(ctx -> new PainlessInfoJson.Context(ctx, commonClassInfos, javaNamesToDisplayNames))
                .collect(Collectors.toList());
        }

        public PainlessInfos(List<PainlessContextInfo> contextInfos, JavadocExtractor extractor) throws IOException {
            javaNamesToDisplayNames = getDisplayNames(contextInfos);

            javaNamesToJavadoc = new HashMap<>();
            javaNamesToArgs = new HashMap<>();

            Set<PainlessContextClassInfo> commonClassInfos = getCommon(contextInfos, PainlessContextInfo::getClasses);
            common = PainlessInfoJson.Class.fromInfos(sortClassInfos(commonClassInfos), javaNamesToDisplayNames, extractor);

            importedMethods = getCommon(contextInfos, PainlessContextInfo::getImportedMethods);

            classBindings = getCommon(contextInfos, PainlessContextInfo::getClassBindings);

            instanceBindings = getCommon(contextInfos, PainlessContextInfo::getInstanceBindings);

            contexts = new ArrayList<>(contextInfos.size());
            for (PainlessContextInfo contextInfo : contextInfos) {
                contexts.add(new PainlessInfoJson.Context(contextInfo, commonClassInfos, javaNamesToDisplayNames, extractor));
            }
        }

        private <T> Set<T> getCommon(List<PainlessContextInfo> contexts, Function<PainlessContextInfo,List<T>> getter) {
            Map<T, Integer> infoCounts = new HashMap<>();
            for (PainlessContextInfo contextInfo : contexts) {
                for (T info : getter.apply(contextInfo)) {
                    infoCounts.merge(info, 1, Integer::sum);
                }
            }
            return infoCounts.entrySet().stream().filter(
                e -> e.getValue() == contexts.size()
            ).map(Map.Entry::getKey).collect(Collectors.toSet());
        }
    }
}
