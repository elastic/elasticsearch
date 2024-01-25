/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.util;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.tree.ClassNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.objectweb.asm.Opcodes.V_PREVIEW;

public class JavaUtil {
    private static final Logger logger = LoggerFactory.getLogger(JavaUtil.class);

    public static void stripPreviewFromFiles(Path compileDir) {
        try (Stream<Path> fileStream = Files.walk(compileDir)) {
            fileStream.filter(p -> p.toString().endsWith(".class"))
                .forEach(JavaUtil::maybeStripPreview);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void maybeStripPreview(Path file) {
        ClassWriter classWriter = null;
        try (var in = Files.newInputStream(file)) {
            ClassReader classReader = new ClassReader(in);
            ClassNode classNode = new ClassNode();
            classReader.accept(classNode, 0);

            if ((classNode.version & V_PREVIEW) != 0) {
                logger.debug("stripping preview bit from " + file);
                classNode.version = classNode.version & ~V_PREVIEW;
                classWriter = new ClassWriter(0);
                classNode.accept(classWriter);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (classWriter != null) {
            try (var out = Files.newOutputStream(file)) {
                out.write(classWriter.toByteArray());
            } catch (IOException e) {
                throw new org.gradle.api.UncheckedIOException(e);
            }
        }

    }
}
