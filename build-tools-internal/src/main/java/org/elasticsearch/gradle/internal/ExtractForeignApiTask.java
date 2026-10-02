/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.jvm.toolchain.JavaLauncher;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkerExecutor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import javax.inject.Inject;

import static org.objectweb.asm.Opcodes.V21;

/**
 * Extracts public API class files from the {@code java.lang.foreign} package and writes them to a
 * JAR with {@code @PreviewFeature} annotations and the class-file preview flag stripped.
 *
 * <p> The output JAR is intended for use with {@code --patch-module java.base=<jar>} so that
 * modules compiled with {@code --release 21} can reference {@code MemorySegment} and related
 * types without {@code --enable-preview}.
 *
 * <p> The worker reads the classes from its own {@code jrt:/} image, which must be a genuine
 * JDK 21 runtime. When the Gradle daemon already runs on JDK 21 the worker executes in-process
 * ({@code noIsolation}); otherwise {@link ForeignApiPlugin} supplies a JDK 21 toolchain launcher
 * via {@link #getJdk21Launcher()} and the worker runs in a forked process started with that
 * launcher. The same pattern is used by
 * {@link org.elasticsearch.gradle.internal.precommit.CheckForbiddenApisTask}.
 *
 * <h3> Inspecting the generated JAR</h3>
 * {@code javap} cannot inspect classes in {@code java.lang.foreign} from a JAR on the classpath
 * because the JDK's module system always resolves them from {@code jrt://java.base} first. To
 * work around this, extract the class file and copy it to a path outside the
 * {@code java/lang/foreign/} directory structure so that {@code javap} reads the file directly:
 * <pre>{@code
 * mkdir -p /tmp/jar-inspect
 * unzip -o -d /tmp/jar-inspect \
 *   libs/native/build/jdk21-foreign-api.jar \
 *   java/lang/foreign/MemorySegment.class
 * cp /tmp/jar-inspect/java/lang/foreign/MemorySegment.class /tmp/jar-inspect/
 * javap -v -p /tmp/jar-inspect/MemorySegment.class
 * }</pre>
 * Key things to verify in the output:
 * <ul>
 *   <li>{@code major version: 65} (Java 21)</li>
 *   <li>{@code minor version: 0} (no preview flag; preview would be 65535)</li>
 *   <li>No {@code @PreviewFeature} in any {@code RuntimeVisibleAnnotations} or
 *       {@code RuntimeInvisibleAnnotations} section (stale constant-pool string references to
 *       {@code PreviewFeature} are harmless)</li>
 * </ul>
 */
@CacheableTask
public abstract class ExtractForeignApiTask extends DefaultTask {

    private static final String PREVIEW_FEATURE_DESCRIPTOR = "Ljdk/internal/javac/PreviewFeature;";
    private static final String FOREIGN_PACKAGE_PREFIX = "java/lang/foreign/";
    private static final int PUBLIC_OR_PROTECTED = Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED;

    @Inject
    public abstract WorkerExecutor getWorkerExecutor();

    @OutputFile
    public abstract RegularFileProperty getOutputJar();

    /**
     * Optional JDK 21 toolchain launcher. When set, the extraction worker runs in a forked
     * process started with this launcher's {@code java} executable. When absent, the worker
     * runs in the Gradle daemon ({@code noIsolation}), which is only safe when the daemon
     * itself is already a JDK 21 runtime. {@link ForeignApiPlugin} populates this property only
     * when the Gradle JVM differs from JDK 21.
     */
    @Nested
    @Optional
    public abstract Property<JavaLauncher> getJdk21Launcher();

    @TaskAction
    public void extract() {
        var workerExecutor = getWorkerExecutor();
        var workQueue = getJdk21Launcher().isPresent()
            ? workerExecutor.processIsolation(
                spec -> spec.forkOptions(opts -> opts.executable(getJdk21Launcher().get().getExecutablePath().getAsFile()))
            )
            : workerExecutor.noIsolation();
        workQueue.submit(ExtractionWorkAction.class, params -> params.getOutputJar().set(getOutputJar()));
    }

    static byte[] createStub(InputStream classStream) throws IOException {
        return ExtractionWorkAction.createStub(classStream);
    }

    // -------------------------------------------------------------------------
    // Worker parameters
    // -------------------------------------------------------------------------

    public interface ExtractionParameters extends WorkParameters {
        RegularFileProperty getOutputJar();
    }

    // -------------------------------------------------------------------------
    // Worker action — runs inside the forked JDK 21 process
    // -------------------------------------------------------------------------

    /**
     * Extraction logic that executes inside the forked JDK 21 worker process.
     * Reads {@code java.lang.foreign} classes from the worker's own {@code jrt:/} image, so there
     * are no cross-JDK module-format compatibility concerns.
     */
    public abstract static class ExtractionWorkAction implements WorkAction<ExtractionParameters> {

        private static final Logger LOGGER = Logging.getLogger(ExtractionWorkAction.class);

        @Override
        public void execute() {
            checkRuntimeJava21();

            Path outputPath = getParameters().getOutputJar().getAsFile().get().toPath();
            try {
                Files.createDirectories(outputPath.getParent());
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create output directory", e);
            }

            FileSystem jrtFs = FileSystems.getFileSystem(URI.create("jrt:/"));
            Path foreignRoot = jrtFs.getPath("modules", "java.base", "java", "lang", "foreign");

            int count = 0;
            try (
                JarOutputStream jar = new JarOutputStream(Files.newOutputStream(outputPath));
                Stream<Path> walk = Files.walk(foreignRoot)
            ) {
                for (Path file : (Iterable<Path>) walk::iterator) {
                    if (Files.isRegularFile(file) == false || file.getFileName().toString().endsWith(".class") == false) {
                        continue;
                    }
                    byte[] stubBytes;
                    try (InputStream is = Files.newInputStream(file)) {
                        stubBytes = createStub(is);
                    }
                    if (stubBytes == null) {
                        continue;
                    }
                    String entryName = FOREIGN_PACKAGE_PREFIX + file.getFileName().toString();
                    JarEntry entry = new JarEntry(entryName);
                    entry.setTime(0);
                    jar.putNextEntry(entry);
                    jar.write(stubBytes);
                    jar.closeEntry();
                    count++;
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to extract foreign API stubs", e);
            }

            LOGGER.info("Generated {} with {} class(es)", outputPath, count);
        }

        private static void checkRuntimeJava21() {
            int jdkVersion = Runtime.version().feature();
            if (jdkVersion != 21) {
                throw new IllegalStateException(
                    "ExtractForeignApiTask worker must run on JDK 21 (found JDK "
                        + jdkVersion
                        + "). "
                        + "The Foreign Function & Memory API is preview in JDK 21 only; on JDK 22+ it is standard and this stub JAR is unnecessary."
                );
            }
        }

        static byte[] createStub(InputStream classStream) throws IOException {
            ClassReader reader = new ClassReader(classStream);
            ClassNode cn = new ClassNode();
            reader.accept(cn, 0);
            checkJava21(cn);

            if ((cn.access & Opcodes.ACC_PUBLIC) == 0) {
                return null;
            }

            cn.version = cn.version & ~Opcodes.V_PREVIEW;

            cn.visibleAnnotations = stripPreviewAnnotations(cn.visibleAnnotations);
            cn.invisibleAnnotations = stripPreviewAnnotations(cn.invisibleAnnotations);

            if (cn.fields != null) {
                cn.fields.removeIf(f -> (f.access & PUBLIC_OR_PROTECTED) == 0);
                for (FieldNode f : cn.fields) {
                    f.visibleAnnotations = stripPreviewAnnotations(f.visibleAnnotations);
                    f.invisibleAnnotations = stripPreviewAnnotations(f.invisibleAnnotations);
                }
            }
            if (cn.methods != null) {
                cn.methods.removeIf(m -> (m.access & PUBLIC_OR_PROTECTED) == 0);
                for (MethodNode m : cn.methods) {
                    m.visibleAnnotations = stripPreviewAnnotations(m.visibleAnnotations);
                    m.invisibleAnnotations = stripPreviewAnnotations(m.invisibleAnnotations);
                    m.instructions.clear();
                    m.tryCatchBlocks.clear();
                    if (m.localVariables != null) {
                        m.localVariables.clear();
                    }
                    m.maxStack = 0;
                    m.maxLocals = 0;
                }
            }

            cn.innerClasses.removeIf(ic -> (ic.access & Opcodes.ACC_PUBLIC) == 0);

            ClassWriter writer = new ClassWriter(0);
            cn.accept(writer);
            return writer.toByteArray();
        }

        private static List<AnnotationNode> stripPreviewAnnotations(List<AnnotationNode> anns) {
            if (anns == null) {
                return null;
            }
            anns.removeIf(a -> a.desc.equals(PREVIEW_FEATURE_DESCRIPTOR));
            return anns.isEmpty() ? null : anns;
        }

        private static void checkJava21(ClassNode cn) {
            int rawVersion = cn.version & 0xFFFF;
            if (rawVersion != V21) {
                throw new IllegalStateException(
                    "Unexpected class file version " + rawVersion + " for " + cn.name + " (expected " + V21 + " / JDK 21)"
                );
            }
        }
    }
}
