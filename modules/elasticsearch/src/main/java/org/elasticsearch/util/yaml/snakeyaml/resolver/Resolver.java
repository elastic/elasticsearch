/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.util.yaml.snakeyaml.resolver;

import org.elasticsearch.util.yaml.snakeyaml.nodes.NodeId;
import org.elasticsearch.util.yaml.snakeyaml.nodes.Tag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Resolver tries to detect a type by scalars's content (when the type is
 * implicit)
 *
 * @see <a href="http://pyyaml.org/wiki/PyYAML">PyYAML</a> for more information
 */
public class Resolver {
    public static final Pattern BOOL = Pattern
            .compile("^(?:yes|Yes|YES|no|No|NO|true|True|TRUE|false|False|FALSE|on|On|ON|off|Off|OFF)$");
    public static final Pattern FLOAT = Pattern
            .compile("^(?:[-+]?(?:[0-9][0-9_]*)\\.[0-9_]*(?:[eE][-+][0-9]+)?|[-+]?(?:[0-9][0-9_]*)?\\.[0-9_]+(?:[eE][-+][0-9]+)?|[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\\.[0-9_]*|[-+]?\\.(?:inf|Inf|INF)|\\.(?:nan|NaN|NAN))$");
    public static final Pattern INT = Pattern
            .compile("^(?:[-+]?0b[0-1_]+|[-+]?0[0-7_]+|[-+]?(?:0|[1-9][0-9_]*)|[-+]?0x[0-9a-fA-F_]+|[-+]?[1-9][0-9_]*(?::[0-5]?[0-9])+)$");
    public static final Pattern MERGE = Pattern.compile("^(?:<<)$");
    public static final Pattern NULL = Pattern.compile("^(?:~|null|Null|NULL| )$");
    public static final Pattern EMPTY = Pattern.compile("^$");
    public static final Pattern TIMESTAMP = Pattern
            .compile("^(?:[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]|[0-9][0-9][0-9][0-9]-[0-9][0-9]?-[0-9][0-9]?(?:[Tt]|[ \t]+)[0-9][0-9]?:[0-9][0-9]:[0-9][0-9](?:\\.[0-9]*)?(?:[ \t]*(?:Z|[-+][0-9][0-9]?(?::[0-9][0-9])?))?)$");
    public static final Pattern VALUE = Pattern.compile("^(?:=)$");
    public static final Pattern YAML = Pattern.compile("^(?:!|&|\\*)$");

    protected Map<Character, List<ResolverTuple>> yamlImplicitResolvers = new HashMap<Character, List<ResolverTuple>>();

    /**
     * Create Resolver
     *
     * @param respectDefaultImplicitScalars false to parse/dump scalars as plain Strings
     * @deprecated override addImplicitResolvers instead
     */
    public Resolver(boolean respectDefaultImplicitScalars) {
        if (respectDefaultImplicitScalars) {
            addImplicitResolvers();
        }
    }

    protected void addImplicitResolvers() {
        addImplicitResolver(Tag.BOOL, BOOL, "yYnNtTfFoO");
        addImplicitResolver(Tag.FLOAT, FLOAT, "-+0123456789.");
        addImplicitResolver(Tag.INT, INT, "-+0123456789");
        addImplicitResolver(Tag.MERGE, MERGE, "<");
        addImplicitResolver(Tag.NULL, NULL, "~nN\0");
        addImplicitResolver(Tag.NULL, EMPTY, null);
        addImplicitResolver(Tag.TIMESTAMP, TIMESTAMP, "0123456789");
        addImplicitResolver(Tag.VALUE, VALUE, "=");
        // The following implicit resolver is only for documentation
        // purposes.
        // It cannot work
        // because plain scalars cannot start with '!', '&', or '*'.
        addImplicitResolver(Tag.YAML, YAML, "!&*");
    }

    public Resolver() {
        this(true);
    }

    public void addImplicitResolver(Tag tag, Pattern regexp, String first) {
        if (first == null) {
            List<ResolverTuple> curr = yamlImplicitResolvers.get(null);
            if (curr == null) {
                curr = new ArrayList<ResolverTuple>();
                yamlImplicitResolvers.put(null, curr);
            }
            curr.add(new ResolverTuple(tag, regexp));
        } else {
            char[] chrs = first.toCharArray();
            for (int i = 0, j = chrs.length; i < j; i++) {
                Character theC = new Character(chrs[i]);
                if (theC == 0) {
                    // special case: for null
                    theC = null;
                }
                List<ResolverTuple> curr = yamlImplicitResolvers.get(theC);
                if (curr == null) {
                    curr = new ArrayList<ResolverTuple>();
                    yamlImplicitResolvers.put(theC, curr);
                }
                curr.add(new ResolverTuple(tag, regexp));
            }
        }
    }

    public Tag resolve(NodeId kind, String value, boolean implicit) {
        if (kind == NodeId.scalar && implicit) {
            List<ResolverTuple> resolvers = null;
            if ("".equals(value)) {
                resolvers = yamlImplicitResolvers.get('\0');
            } else {
                resolvers = yamlImplicitResolvers.get(value.charAt(0));
            }
            if (resolvers != null) {
                for (ResolverTuple v : resolvers) {
                    Tag tag = v.getTag();
                    Pattern regexp = v.getRegexp();
                    if (regexp.matcher(value).matches()) {
                        return tag;
                    }
                }
            }
            if (yamlImplicitResolvers.containsKey(null)) {
                for (ResolverTuple v : yamlImplicitResolvers.get(null)) {
                    Tag tag = v.getTag();
                    Pattern regexp = v.getRegexp();
                    if (regexp.matcher(value).matches()) {
                        return tag;
                    }
                }
            }
        }
        switch (kind) {
            case scalar:
                return Tag.STR;
            case sequence:
                return Tag.SEQ;
            default:
                return Tag.MAP;
        }
    }
}