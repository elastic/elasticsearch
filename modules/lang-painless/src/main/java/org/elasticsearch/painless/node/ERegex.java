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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.CallNode;
import org.elasticsearch.painless.ir.CallSubNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.FieldNode;
import org.elasticsearch.painless.ir.MemberFieldLoadNode;
import org.elasticsearch.painless.ir.MemberFieldStoreNode;
import org.elasticsearch.painless.ir.StatementExpressionNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Represents a regex constant. All regexes are constants.
 */
public class ERegex extends AExpression {

    protected final String pattern;
    protected final String flags;

    public ERegex(Location location, String pattern, String flags) {
        super(location);

        this.pattern = Objects.requireNonNull(pattern);
        this.flags = flags;
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to regex constant [" + pattern + "] with flags [" + flags + "]"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: regex constant [" + pattern + "] with flags [" + flags + "] not used"));
        }

        Output output = new Output();

        if (scriptRoot.getCompilerSettings().areRegexesEnabled() == false) {
            throw createError(new IllegalStateException("Regexes are disabled. Set [script.painless.regex.enabled] to [true] "
                    + "in elasticsearch.yaml to allow them. Be careful though, regexes break out of Painless's protection against deep "
                    + "recursion and long loops."));
        }

        int flags = 0;

        for (int c = 0; c < this.flags.length(); c++) {
            flags |= flagForChar(this.flags.charAt(c));
        }

        try {
            Pattern.compile(pattern, flags);
        } catch (PatternSyntaxException e) {
            throw new Location(location.getSourceName(), location.getOffset() + 1 + e.getIndex()).createError(
                    new IllegalArgumentException("Error compiling regex: " + e.getDescription()));
        }

        String name = scriptRoot.getNextSyntheticName("regex");
        output.actual = Pattern.class;

        FieldNode fieldNode = new FieldNode();
        fieldNode.setLocation(location);
        fieldNode.setModifiers(Modifier.FINAL | Modifier.STATIC | Modifier.PRIVATE);
        fieldNode.setFieldType(Pattern.class);
        fieldNode.setName(name);

        classNode.addFieldNode(fieldNode);

        try {
            StatementExpressionNode statementExpressionNode = new StatementExpressionNode();
            statementExpressionNode.setLocation(location);

            BlockNode blockNode = classNode.getClinitBlockNode();
            blockNode.addStatementNode(statementExpressionNode);

            MemberFieldStoreNode memberFieldStoreNode = new MemberFieldStoreNode();
            memberFieldStoreNode.setLocation(location);
            memberFieldStoreNode.setExpressionType(void.class);
            memberFieldStoreNode.setFieldType(Pattern.class);
            memberFieldStoreNode.setName(name);
            memberFieldStoreNode.setStatic(true);

            statementExpressionNode.setExpressionNode(memberFieldStoreNode);

            CallNode callNode = new CallNode();
            callNode.setLocation(location);
            callNode.setExpressionType(Pattern.class);

            memberFieldStoreNode.setChildNode(callNode);

            StaticNode staticNode = new StaticNode();
            staticNode.setLocation(location);
            staticNode.setExpressionType(Pattern.class);

            callNode.setLeftNode(staticNode);

            CallSubNode callSubNode = new CallSubNode();
            callSubNode.setLocation(location);
            callSubNode.setExpressionType(Pattern.class);
            callSubNode.setBox(Pattern.class);
            callSubNode.setMethod(new PainlessMethod(
                    Pattern.class.getMethod("compile", String.class, int.class),
                    Pattern.class,
                    Pattern.class,
                    Arrays.asList(String.class, int.class),
                    null,
                    null,
                    null
                    )
            );

            callNode.setRightNode(callSubNode);

            ConstantNode constantNode = new ConstantNode();
            constantNode.setLocation(location);
            constantNode.setExpressionType(String.class);
            constantNode.setConstant(pattern);

            callSubNode.addArgumentNode(constantNode);

            constantNode = new ConstantNode();
            constantNode.setLocation(location);
            constantNode.setExpressionType(int.class);
            constantNode.setConstant(flags);

            callSubNode.addArgumentNode(constantNode);
        } catch (Exception exception) {
            throw createError(new IllegalStateException("could not generate regex constant [" + pattern + "/" + flags +"] in clinit"));
        }

        MemberFieldLoadNode memberFieldLoadNode = new MemberFieldLoadNode();
        memberFieldLoadNode.setLocation(location);
        memberFieldLoadNode.setExpressionType(Pattern.class);
        memberFieldLoadNode.setName(name);
        memberFieldLoadNode.setStatic(true);

        output.expressionNode = memberFieldLoadNode;

        return output;
    }

    private int flagForChar(char c) {
        switch (c) {
            case 'c': return Pattern.CANON_EQ;
            case 'i': return Pattern.CASE_INSENSITIVE;
            case 'l': return Pattern.LITERAL;
            case 'm': return Pattern.MULTILINE;
            case 's': return Pattern.DOTALL;
            case 'U': return Pattern.UNICODE_CHARACTER_CLASS;
            case 'u': return Pattern.UNICODE_CASE;
            case 'x': return Pattern.COMMENTS;
            default:
                throw new IllegalArgumentException("Unknown flag [" + c + "]");
        }
    }
}
