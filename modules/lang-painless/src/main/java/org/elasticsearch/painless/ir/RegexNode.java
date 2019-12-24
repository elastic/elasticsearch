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

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Constant;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.WriterConstants;

import java.util.regex.Pattern;

public class RegexNode extends ExpressionNode {

    /* ---- begin tree structure ---- */

    @Override
    public RegexNode setTypeNode(TypeNode typeNode) {
        super.setTypeNode(typeNode);
        return this;
    }

    /* ---- end tree structure, begin node data ---- */

    protected String pattern;
    protected int flags;
    protected Constant constant;

    public RegexNode setPattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    public String getPattern() {
        return pattern;
    }

    public RegexNode setFlags(int flags) {
        this.flags = flags;
        return this;
    }

    public int getFlags() {
        return flags;
    }

    public RegexNode setConstant(Constant constant) {
        this.constant = constant;
        return this;
    }

    public Object getConstant() {
        return constant;
    }

    @Override
    public RegexNode setLocation(Location location) {
        super.setLocation(location);
        return this;
    }

    /* ---- end node data ---- */

    public RegexNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        methodWriter.getStatic(WriterConstants.CLASS_TYPE, constant.name, org.objectweb.asm.Type.getType(Pattern.class));
        globals.addConstantInitializer(constant);
    }

    public void initializeConstant(MethodWriter writer) {
        writer.push(pattern);
        writer.push(flags);
        writer.invokeStatic(org.objectweb.asm.Type.getType(Pattern.class), WriterConstants.PATTERN_COMPILE);
    }

    protected int flagForChar(char c) {
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
