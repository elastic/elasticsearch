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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;

/**
 * Analyzes methods for broken catch blocks. These are catch blocks that somehow
 * drop the original exception (via any possible codepath). It is ok to wrap the exception
 * with another one, e.g.:
 * <ul>
 *   <li>new OtherException(..., original, ...)
 *   <li>otherException.initCause(original)
 *   <li>otherException.addSuppressed(original)
 * </ul>
 */
public class CatchAnalyzer {
 
    /** Exception thrown when some exceptions are swallowed */
    @SuppressWarnings("serial")
    public static class SwallowedException extends RuntimeException {
        public SwallowedException(String message) {
            super(message);
        }
    }
    
    static final int ASM_API_VERSION = Opcodes.ASM6;
    
    /** Main method, takes directories as parameter. These must also be on classpath!!!! */
    public static void main(String args[]) throws Exception {
        long violationCount = 0;
        long scannedCount = 0;
        long startTime = System.currentTimeMillis();
        List<Path> files = new ArrayList<>();
        if (args.length < 3) {
            throw new IllegalArgumentException("CatchAnalyzer -classpath <path> <dirs>");
        }
        if (args[0].equals("-classpath") == false) {
            throw new IllegalArgumentException("CatchAnalyzer -classpath <path> <dirs>");
        }
        
        ClassLoader loader = createLoader(args[1]);

        // step 1: collect files
        for (int i = 2; i < args.length; i++) {
            String arg = args[i];
            Path dir = Paths.get(arg);
            Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.toString().endsWith(".class")) {
                        files.add(file);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        // step 2: sort
        files.sort((x,y) -> x.toAbsolutePath().toString().compareTo(y.toAbsolutePath().toString()));
        // step 3: process
        for (Path file : files) {
            byte bytes[] = Files.readAllBytes(file);
            ClassReader reader = new ClassReader(bytes);
            // entirely synthetic class, e.g. enum switch table, which always masks NoSuchFieldError!!!!!
            if ((reader.getAccess() & Opcodes.ACC_SYNTHETIC) != 0) {
                continue;
            }
            ClassAnalyzer analyzer = new ClassAnalyzer(reader.getClassName(), loader);
            reader.accept(analyzer, 0);
            scannedCount++;
            
            violationCount += analyzer.logViolations(System.out);
        }
        long endTime = System.currentTimeMillis();
        // step 4: print        
        
        System.out.println("Scanned " + scannedCount + " classes in " + (endTime - startTime) + " ms");
        if (violationCount > 0) {
            throw new SwallowedException(violationCount + " violations were found, see log for more details");
        }
    }
    
    /**
     * Creates a classloader for the provided classpath
     */
    static ClassLoader createLoader(String classPath) {
        String pathSeparator = System.getProperty("path.separator");
        String fileSeparator = System.getProperty("file.separator");
        String elements[] = classPath.split(pathSeparator);
        URL urlElements[] = new URL[elements.length];
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            if (element.isEmpty()) {
                element = System.getProperty("user.dir");
            }
            // we should be able to just Paths.get() each element, but unfortunately this is not the
            // whole story on how classpath parsing works: if you want to know, start at sun.misc.Launcher,
            // be sure to stop before you tear out your eyes. we just handle the "alternative" filename
            // specification which java seems to allow, explicitly, right here...
            if (element.startsWith("/") && "\\".equals(fileSeparator)) {
                // "correct" the entry to become a normal entry
                // change to correct file separators
                element = element.replace("/", "\\");
                // if there is a drive letter, nuke the leading separator
                if (element.length() >= 3 && element.charAt(2) == ':') {
                    element = element.substring(1);
                }
            }
            // now just parse as ordinary file
            try {
                urlElements[i] = Paths.get(element).toUri().toURL();
            } catch (MalformedURLException e) {
                // should not happen, as we use the filesystem API
                throw new RuntimeException(e);
            }
        }
        // set up a parallel loader
        ClassLoader parent = ClassLoader.getSystemClassLoader().getParent();
        if (parent == null) {
            throw new UnsupportedOperationException("we can't safely scan for you, sorry");
        }
        return URLClassLoader.newInstance(urlElements, parent);
    }
}
