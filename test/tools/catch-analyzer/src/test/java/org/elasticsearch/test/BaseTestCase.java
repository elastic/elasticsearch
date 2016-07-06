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

import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;

import org.junit.Assert;
import org.junit.runner.RunWith;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

@TestMethodProviders({
    LuceneJUnit3MethodProvider.class,
    JUnit4MethodProvider.class
})
@RunWith(RandomizedRunner.class)
public class BaseTestCase extends Assert {
    
    /** analyzes the method, and returns its analysis */
    public MethodAnalyzer analyze(Method method) throws Exception {
        return analyze(method.getDeclaringClass(), method.getName(), Type.getMethodDescriptor(method));
    }
    
    /** analyzes the ctor, and returns its analysis */
    public MethodAnalyzer analyze(Constructor<?> method) throws Exception {
        return analyze(method.getDeclaringClass(), "<init>", Type.getConstructorDescriptor(method));
    }
    
    /** analyzes the method, and returns its analysis */
    private MethodAnalyzer analyze(Class<?> parentClass, String methodName, String methodDesc) throws Exception {
        ClassReader reader = new ClassReader(parentClass.getName());
        AtomicLong analyzedMethods = new AtomicLong();
        MethodAnalyzer analyzer[] = new MethodAnalyzer[1];
        reader.accept(new ClassVisitor(CatchAnalyzer.ASM_API_VERSION, null) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                if (name.equals(methodName) && desc.equals(methodDesc)) {
                    analyzedMethods.incrementAndGet();
                    return analyzer[0] = new MethodAnalyzer(getClass().getClassLoader(), reader.getClassName(), 
                                                            reader.getSuperName(), access, name, desc, signature, exceptions);
                }
                return null;
            }
        }, 0);
        assertNotNull("method not found", analyzer[0]);
        return analyzer[0];
    }
}
