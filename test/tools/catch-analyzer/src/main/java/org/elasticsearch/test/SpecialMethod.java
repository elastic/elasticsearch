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

package org.elasticsearch.test;

import org.objectweb.asm.tree.MethodInsnNode;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** simple whitelist of special helper methods that preserve exceptions */
class SpecialMethod {
    private final String owner;
    private final String name;
    private final String desc;
    
    private SpecialMethod(String owner, String name, String desc) {
        this.owner = owner;
        this.name = name;
        this.desc = desc;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((desc == null) ? 0 : desc.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((owner == null) ? 0 : owner.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        SpecialMethod other = (SpecialMethod) obj;
        if (desc == null) {
            if (other.desc != null) return false;
        } else if (!desc.equals(other.desc)) return false;
        if (name == null) {
            if (other.name != null) return false;
        } else if (!name.equals(other.name)) return false;
        if (owner == null) {
            if (other.owner != null) return false;
        } else if (!owner.equals(other.owner)) return false;
        return true;
    }

    /** These are "boxers", they return a value that preserves the exception */
    private static final Set<SpecialMethod> BOXERS = new HashSet<>(Arrays.asList(
            new SpecialMethod("org/elasticsearch/ExceptionsHelper", 
                              "convertToElastic", 
                              "(Ljava/lang/Throwable;)Lorg/elasticsearch/ElasticsearchException;"),
            new SpecialMethod("org/elasticsearch/ExceptionsHelper", 
                              "convertToRuntime", 
                              "(Ljava/lang/Throwable;)Ljava/lang/RuntimeException;"),
            new SpecialMethod("org/elasticsearch/ExceptionsHelper", 
                              "useOrSuppress", 
                              "(Ljava/lang/Throwable;Ljava/lang/Throwable;)Ljava/lang/Throwable;")
    ));
    
    /** These are rethrowers, they actually throw the passed exception (possibly boxing too, but preserve it) */
    private static final Set<SpecialMethod> RETHROWERS = new HashSet<>(Arrays.asList(
            new SpecialMethod("org/apache/lucene/util/IOUtils", 
                              "reThrow", 
                              "(Ljava/lang/Throwable;)V"),
            new SpecialMethod("org/apache/lucene/util/IOUtils", 
                              "reThrowUnchecked", 
                              "(Ljava/lang/Throwable;)V"),
            // TODO: not really a rethrower, only rethrows if arg is non-null
            new SpecialMethod("org/apache/lucene/codecs/CodecUtil", 
                              "checkFooter", 
                              "(Lorg/apache/lucene/store/ChecksumIndexInput;Ljava/lang/Throwable;)V")
    ));
    
    /** True if the insn is a call to a boxer (returns the original, or a wrapper) */
    static boolean isBoxer(MethodInsnNode insn) {
        return BOXERS.contains(new SpecialMethod(insn.owner, insn.name, insn.desc));
    }
    
    /** True if the insn is a call to an exception rethrower */
    static boolean isRethrower(MethodInsnNode insn) {
        return RETHROWERS.contains(new SpecialMethod(insn.owner, insn.name, insn.desc));
    }
}
