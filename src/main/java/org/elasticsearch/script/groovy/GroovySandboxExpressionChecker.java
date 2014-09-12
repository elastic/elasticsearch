/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script.groovy;

import com.google.common.collect.ImmutableSet;
import org.codehaus.groovy.ast.ClassNode;
import org.codehaus.groovy.ast.expr.ConstructorCallExpression;
import org.codehaus.groovy.ast.expr.Expression;
import org.codehaus.groovy.ast.expr.GStringExpression;
import org.codehaus.groovy.ast.expr.MethodCallExpression;
import org.codehaus.groovy.control.customizers.SecureASTCustomizer;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Class used to determine whether a Groovy expression should be allowed.
 * During compilation, every expression is passed to the
 * <code>isAuthorized</code> method, which returns true to allow that method
 * and false to block it. Includes all of the sandbox-related whitelist and
 * blacklist options.
 */
public class GroovySandboxExpressionChecker implements SecureASTCustomizer.ExpressionChecker {

    public static String GROOVY_SANDBOX_METHOD_BLACKLIST = "script.groovy.sandbox.method_blacklist";
    public static String GROOVY_SANDBOX_PACKAGE_WHITELIST = "script.groovy.sandbox.package_whitelist";
    public static String GROOVY_SANDBOX_CLASS_WHITELIST = "script.groovy.sandbox.class_whitelist";
    public static String GROOVY_SCRIPT_SANDBOX_RECEIVER_WHITELIST = "script.groovy.sandbox.receiver_whitelist";

    private final Set<String> methodBlacklist;
    private final Set<String> packageWhitelist;
    private final Set<String> classWhitelist;

    public GroovySandboxExpressionChecker(Settings settings) {
        this.methodBlacklist = ImmutableSet.copyOf(settings.getAsArray(GROOVY_SANDBOX_METHOD_BLACKLIST, defaultMethodBlacklist, true));
        this.packageWhitelist = ImmutableSet.copyOf(settings.getAsArray(GROOVY_SANDBOX_PACKAGE_WHITELIST, defaultPackageWhitelist, true));
        this.classWhitelist = ImmutableSet.copyOf(settings.getAsArray(GROOVY_SANDBOX_CLASS_WHITELIST, defaultClassConstructionWhitelist, true));
    }

    // Never allow calling these methods, regardless of the object type
    public static String[] defaultMethodBlacklist = new String[]{
            "getClass",
            "wait",
            "notify",
            "notifyAll",
            "finalize"
    };

    // Only instances of these classes in these packages can be instantiated
    public static String[] defaultPackageWhitelist = new String[] {"java.util", "java.lang", "org.joda.time"};

    // Classes that are allowed to be constructed
    public static String[] defaultClassConstructionWhitelist = new String[]{
            java.util.Date.class.getName(),
            java.util.Map.class.getName(),
            java.util.List.class.getName(),
            java.util.Set.class.getName(),
            java.util.ArrayList.class.getName(),
            java.util.Arrays.class.getName(),
            java.util.HashMap.class.getName(),
            java.util.HashSet.class.getName(),
            java.util.UUID.class.getName(),
            java.math.BigDecimal.class.getName(),
            org.joda.time.DateTime.class.getName(),
            org.joda.time.DateTimeZone.class.getName()
    };

    // Default whitelisted receiver classes for the Groovy sandbox
    private final static String[] defaultReceiverWhitelist = new String [] {
            groovy.util.GroovyCollections.class.getName(),
            java.lang.Math.class.getName(),
            java.lang.Integer.class.getName(), "[I", "[[I", "[[[I",
            java.lang.Float.class.getName(), "[F", "[[F", "[[[F",
            java.lang.Double.class.getName(), "[D", "[[D", "[[[D",
            java.lang.Long.class.getName(), "[J", "[[J", "[[[J",
            java.lang.Short.class.getName(), "[S", "[[S", "[[[S",
            java.lang.Character.class.getName(), "[C", "[[C", "[[[C",
            java.lang.Byte.class.getName(), "[B", "[[B", "[[[B",
            java.lang.Boolean.class.getName(), "[Z", "[[Z", "[[[Z",
            java.math.BigDecimal.class.getName(),
            java.util.Arrays.class.getName(),
            java.util.Date.class.getName(),
            java.util.List.class.getName(),
            java.util.Map.class.getName(),
            java.util.Set.class.getName(),
            java.lang.Object.class.getName(),
            org.joda.time.DateTime.class.getName(),
            org.joda.time.DateTimeUtils.class.getName(),
            org.joda.time.DateTimeZone.class.getName(),
            org.joda.time.Instant.class.getName()
    };

    /**
     * Checks whether the expression to be compiled is allowed
     */
    @Override
    public boolean isAuthorized(Expression expression) {
        if (expression instanceof MethodCallExpression) {
            MethodCallExpression mce = (MethodCallExpression) expression;
            String methodName = mce.getMethodAsString();
            if (methodBlacklist.contains(methodName)) {
                return false;
            } else if (methodName == null && mce.getMethod() instanceof GStringExpression) {
                // We do not allow GStrings for method invocation, they are a security risk
                return false;
            }
        } else if (expression instanceof ConstructorCallExpression) {
            ConstructorCallExpression cce = (ConstructorCallExpression) expression;
            ClassNode type = cce.getType();
            if (!packageWhitelist.contains(type.getPackageName())) {
                return false;
            }
            if (!classWhitelist.contains(type.getName())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a customized ASTCustomizer that includes the whitelists and
     * expression checker.
     */
    public static SecureASTCustomizer getSecureASTCustomizer(Settings settings) {
        SecureASTCustomizer scz = new SecureASTCustomizer();
        // Closures are allowed
        scz.setClosuresAllowed(true);
        // But defining methods is not
        scz.setMethodDefinitionAllowed(false);
        // Only allow the imports that we explicitly call out
        List<String> importWhitelist = new ArrayList<>();
        importWhitelist.addAll(ImmutableSet.copyOf(GroovySandboxExpressionChecker.defaultClassConstructionWhitelist));
        scz.setImportsWhitelist(importWhitelist);
        // Package definitions are not allowed
        scz.setPackageAllowed(false);
        // White-listed receivers of method calls
        String[] receiverWhitelist = settings.getAsArray(GROOVY_SCRIPT_SANDBOX_RECEIVER_WHITELIST, defaultReceiverWhitelist, true);
        scz.setReceiversWhiteList(newArrayList(receiverWhitelist));
        // Add the customized expression checker for finer-grained checking
        scz.addExpressionCheckers(new GroovySandboxExpressionChecker(settings));
        return scz;
    }
}
