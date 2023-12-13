/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import de.thetaphi.forbiddenapis.Checker;
import de.thetaphi.forbiddenapis.Constants;
import de.thetaphi.forbiddenapis.ForbiddenApiException;
import de.thetaphi.forbiddenapis.Logger;
import de.thetaphi.forbiddenapis.ParseException;
import groovy.lang.Closure;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Transformer;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.FileTreeElement;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.VerificationException;
import org.gradle.api.tasks.VerificationTask;
import org.gradle.api.tasks.util.PatternFilterable;
import org.gradle.api.tasks.util.PatternSet;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.RetentionPolicy;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import javax.inject.Inject;

import static de.thetaphi.forbiddenapis.Checker.Option.DISABLE_CLASSLOADING_CACHE;
import static de.thetaphi.forbiddenapis.Checker.Option.FAIL_ON_MISSING_CLASSES;
import static de.thetaphi.forbiddenapis.Checker.Option.FAIL_ON_UNRESOLVABLE_SIGNATURES;
import static de.thetaphi.forbiddenapis.Checker.Option.FAIL_ON_VIOLATION;

@CacheableTask
public abstract class CheckForbiddenApisTask extends DefaultTask implements PatternFilterable, VerificationTask, Constants {

    public static final Set<String> BUNDLED_SIGNATURE_DEFAULTS = Set.of("jdk-unsafe", "jdk-non-portable", "jdk-system-out");

    private static final String NL = System.getProperty("line.separator", "\n");
    private final PatternSet patternSet = new PatternSet().include("**/*.class");
    private FileCollection classesDirs;
    private FileCollection classpath;
    private String targetCompatibility;

    private FileCollection signaturesFiles;

    private final ObjectFactory objectFactory;
    private ProjectLayout projectLayout;

    private List<String> signatures = new ArrayList<>();

    private File resourcesDir;

    private boolean ignoreFailures = false;

    @Input
    @Optional
    abstract SetProperty<String> getBundledSignatures();

    /**
     * List of a custom Java annotations (full class names) that are used in the checked
     * code to suppress errors. Those annotations must have at least
     * {@link RetentionPolicy#CLASS}. They can be applied to classes, their methods,
     * or fields. By default, {@code @de.thetaphi.forbiddenapis.SuppressForbidden}
     * can always be used, but needs the {@code forbidden-apis.jar} file in classpath
     * of compiled project, which may not be wanted.
     * Instead of a full class name, a glob pattern may be used (e.g.,
     * {@code **.SuppressForbidden}).
     */
    @Input
    @Optional
    abstract SetProperty<String> getSuppressAnnotations();

    @Inject
    public CheckForbiddenApisTask(ObjectFactory factory, ProjectLayout projectLayout) {
        signaturesFiles = factory.fileCollection();
        this.objectFactory = factory;
        this.projectLayout = projectLayout;
    }

    @OutputFile
    public File getSuccessMarker() {
        return new File(projectLayout.getBuildDirectory().getAsFile().get(), "markers/" + this.getName());
    }

    /**
     * Directories with the class files to check.
     * Defaults to current sourseSet's output directory (Gradle 3) or output directories (Gradle 4.0+).
     */
    @Internal
    public FileCollection getClassesDirs() {
        return classesDirs;
    }

    /** @see #getClassesDirs() */
    public void setClassesDirs(FileCollection classesDirs) {
        Objects.requireNonNull(classesDirs, "classesDirs");
        this.classesDirs = classesDirs;
    }

    /** Returns the pattern set to match against class files in {@link #getClassesDirs()}. */
    @Internal
    public PatternSet getPatternSet() {
        return patternSet;
    }

    /** @see #getPatternSet() */
    public void setPatternSet(PatternSet patternSet) {
        patternSet.copyFrom(patternSet);
    }

    /**
     * A {@link FileCollection} used to configure the classpath.
     * Defaults to current sourseSet's compile classpath.
     */
    @CompileClasspath
    public FileCollection getClasspath() {
        return classpath;
    }

    /** @see #getClasspath */
    public void setClasspath(FileCollection classpath) {
        Objects.requireNonNull(classpath, "classpath");
        this.classpath = classpath;
    }

    /**
     * A {@link FileCollection} containing all files, which contain signatures and comments for forbidden API calls.
     * The signatures are resolved against {@link #getClasspath()}.
     */
    @InputFiles
    @Optional
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileCollection getSignaturesFiles() {
        return signaturesFiles;
    }

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public File getResourcesDir() {
        return resourcesDir;
    }

    public void setResourcesDir(File resourcesDir) {
        this.resourcesDir = resourcesDir;
    }

    /** @see #getSignaturesFiles */
    public void setSignaturesFiles(FileCollection signaturesFiles) {
        this.signaturesFiles = signaturesFiles;
    }

    public void modifyBundledSignatures(Transformer<Set<String>, Set<String>> transformer) {
        getBundledSignatures().set(transformer.transform(getBundledSignatures().get()));
    }

    public void replaceSignatureFiles(String... signatureFiles) {
        List<File> resources = new ArrayList<>(signatureFiles.length);
        for (Object name : signatureFiles) {
            resources.add(new File(resourcesDir, "forbidden/" + name + ".txt"));
        }
        setSignaturesFiles(objectFactory.fileCollection().from(resources));
    }

    public void addSignatureFiles(String... signatureFiles) {
        List<File> resources = new ArrayList<>(signatureFiles.length);
        for (Object name : signatureFiles) {
            resources.add(new File(resourcesDir, "forbidden/" + name + ".txt"));
        }
        setSignaturesFiles(objectFactory.fileCollection().from(getSignaturesFiles()).from(resources));

    }

    /**
     * Gives multiple API signatures that are joined with newlines and
     * parsed like a single {@link #getSignaturesFiles()}.
     * The signatures are resolved against {@link #getClasspath()}.
     */
    @Input
    @Optional
    public List<String> getSignatures() {
        return signatures;
    }

    /** @see #getSignatures */
    public void setSignatures(List<String> signatures) {
        this.signatures = signatures;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This setting is to conform with {@link VerificationTask} interface.
     * Default is {@code false}.
     */
    @Override
    @Input
    public boolean getIgnoreFailures() {
        return ignoreFailures;
    }

    @Override
    public void setIgnoreFailures(boolean ignoreFailures) {
        this.ignoreFailures = ignoreFailures;
    }

    /**
     * The default compiler target version used to expand references to bundled JDK signatures.
     * E.g., if you use "jdk-deprecated", it will expand to this version.
     * This setting should be identical to the target version used in the compiler task.
     * Defaults to {@code project.targetCompatibility}.
     */
    @Input
    @Optional
    public String getTargetCompatibility() {
        return targetCompatibility;
    }

    /** @see #getTargetCompatibility */
    public void setTargetCompatibility(String targetCompatibility) {
        this.targetCompatibility = targetCompatibility;
    }

    // PatternFilterable implementation:

    /**
     * {@inheritDoc}
     * <p>
     * Set of patterns matching all class files to be parsed from the classesDirectory.
     * Can be changed to e.g. exclude several files (using excludes).
     * The default is a single include with pattern '**&#47;*.class'
     */
    @Override
    @Internal
    public Set<String> getIncludes() {
        return getPatternSet().getIncludes();
    }

    @Override
    public CheckForbiddenApisTask setIncludes(Iterable<String> includes) {
        getPatternSet().setIncludes(includes);
        return this;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Set of patterns matching class files to be excluded from checking.
     */
    @Override
    @Internal
    public Set<String> getExcludes() {
        return getPatternSet().getExcludes();
    }

    @Override
    public CheckForbiddenApisTask setExcludes(Iterable<String> excludes) {
        getPatternSet().setExcludes(excludes);
        return this;
    }

    @Override
    public CheckForbiddenApisTask exclude(String... arg0) {
        getPatternSet().exclude(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask exclude(Iterable<String> arg0) {
        getPatternSet().exclude(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask exclude(Spec<FileTreeElement> arg0) {
        getPatternSet().exclude(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask exclude(@SuppressWarnings("rawtypes") Closure arg0) {
        getPatternSet().exclude(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask include(String... arg0) {
        getPatternSet().include(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask include(Iterable<String> arg0) {
        getPatternSet().include(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask include(Spec<FileTreeElement> arg0) {
        getPatternSet().include(arg0);
        return this;
    }

    @Override
    public CheckForbiddenApisTask include(@SuppressWarnings("rawtypes") Closure arg0) {
        getPatternSet().include(arg0);
        return this;
    }

    /** Returns the classes to check. */
    @InputFiles
    @SkipWhenEmpty
    @IgnoreEmptyDirectories
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileTree getClassFiles() {
        return getClassesDirs().getAsFileTree().matching(getPatternSet());
    }

    @Inject
    public abstract WorkerExecutor getWorkerExecutor();

    /** Executes the forbidden apis task. */
    @TaskAction
    public void checkForbidden() {
        WorkQueue workQueue = getWorkerExecutor().noIsolation();
        workQueue.submit(ForbiddenApisCheckWorkAction.class, parameters -> {
            parameters.getClasspath().setFrom(getClasspath());
            parameters.getClassDirectories().setFrom(getClassesDirs());
            parameters.getClassFiles().from(getClassFiles().getFiles());
            parameters.getSuppressAnnotations().set(getSuppressAnnotations());
            parameters.getBundledSignatures().set(getBundledSignatures());
            parameters.getSignatures().set(getSignatures());
            parameters.getTargetCompatibility().set(getTargetCompatibility());
            parameters.getIgnoreFailures().set(getIgnoreFailures());
            parameters.getSuccessMarker().set(getSuccessMarker());
            parameters.getSignaturesFiles().from(getSignaturesFiles());
        });
    }

    abstract static class ForbiddenApisCheckWorkAction implements WorkAction<Parameters> {

        private final org.gradle.api.logging.Logger logger = Logging.getLogger(getClass());

        @Inject
        public ForbiddenApisCheckWorkAction() {}

        private boolean checkIsUnsupportedJDK(Checker checker) {
            if (checker.isSupportedJDK == false) {
                final String msg = String.format(
                    Locale.ENGLISH,
                    "Your Java runtime (%s %s) is not supported by the forbiddenapis plugin. Please run the checks with a supported JDK!",
                    System.getProperty("java.runtime.name"),
                    System.getProperty("java.runtime.version")
                );
                logger.warn(msg);
                return true;
            }
            return false;
        }

        @Override
        public void execute() {

            final URLClassLoader urlLoader = createClassLoader(getParameters().getClasspath(), getParameters().getClassDirectories());
            try {
                final Checker checker = createChecker(urlLoader);
                if (checkIsUnsupportedJDK(checker)) {
                    return;
                }

                final Set<String> suppressAnnotations = getParameters().getSuppressAnnotations().get();
                for (String a : suppressAnnotations) {
                    checker.addSuppressAnnotation(a);
                }

                try {
                    final Set<String> bundledSignatures = getParameters().getBundledSignatures().get();
                    if (bundledSignatures.isEmpty() == false) {
                        final String bundledSigsJavaVersion = getParameters().getTargetCompatibility().get();
                        if (bundledSigsJavaVersion == null) {
                            logger.warn(
                                "The 'targetCompatibility' project or task property is missing. "
                                    + "Trying to read bundled JDK signatures without compiler target. "
                                    + "You have to explicitly specify the version in the resource name."
                            );
                        }
                        for (String bs : bundledSignatures) {
                            checker.addBundledSignatures(bs, bundledSigsJavaVersion);
                        }
                    }

                    final FileCollection signaturesFiles = getParameters().getSignaturesFiles();
                    if (signaturesFiles != null) for (final File f : signaturesFiles) {
                        checker.parseSignaturesFile(f);
                    }
                    final List<String> signatures = getParameters().getSignatures().get();
                    if ((signatures != null) && !signatures.isEmpty()) {
                        final StringBuilder sb = new StringBuilder();
                        for (String line : signatures) {
                            sb.append(line).append(NL);
                        }
                        checker.parseSignaturesString(sb.toString());
                    }
                } catch (IOException ioe) {
                    throw new GradleException("IO problem while reading files with API signatures.", ioe);
                } catch (ParseException pe) {
                    throw new InvalidUserDataException("Parsing signatures failed: " + pe.getMessage(), pe);
                }

                if (checker.hasNoSignatures()) {
                    if (checker.noSignaturesFilesParsed()) {
                        throw new InvalidUserDataException(
                            "No signatures were added to task; use properties 'signatures', 'bundledSignatures', 'signaturesURLs', and/or 'signaturesFiles' to define those!"
                        );
                    } else {
                        logger.info("Skipping execution because no API signatures are available.");
                        return;
                    }
                }

                try {
                    checker.addClassesToCheck(getParameters().getClassFiles());
                } catch (IOException ioe) {
                    throw new GradleException("Failed to load one of the given class files.", ioe);
                }
                checker.run();
                writeMarker(getParameters().getSuccessMarker().getAsFile().get());
            } catch (ForbiddenApiException e) {
                throw new VerificationException("Forbidden API verification failed", e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                // Close the classloader to free resources:
                try {
                    if (urlLoader != null) urlLoader.close();
                } catch (IOException ioe) {
                    // getLogger().warn("Cannot close classloader: ".concat(ioe.toString()));
                }
            }
        }

        private void writeMarker(File successMarker) throws IOException {
            Files.write(successMarker.toPath(), new byte[] {}, StandardOpenOption.CREATE);
        }

        private URLClassLoader createClassLoader(FileCollection classpath, FileCollection classesDirs) {
            if (classesDirs == null || classpath == null) {
                throw new InvalidUserDataException("Missing 'classesDirs' or 'classpath' property.");
            }

            final Set<File> cpElements = new LinkedHashSet<>();
            cpElements.addAll(classpath.getFiles());
            cpElements.addAll(classesDirs.getFiles());
            final URL[] urls = new URL[cpElements.size()];
            try {
                int i = 0;
                for (final File cpElement : cpElements) {
                    urls[i++] = cpElement.toURI().toURL();
                }
                assert i == urls.length;
            } catch (MalformedURLException mfue) {
                throw new InvalidUserDataException("Failed to build classpath URLs.", mfue);
            }

            return URLClassLoader.newInstance(urls, ClassLoader.getSystemClassLoader());
        }

        @NotNull
        private Checker createChecker(URLClassLoader urlLoader) {
            final EnumSet<Checker.Option> options = EnumSet.noneOf(Checker.Option.class);
            options.add(FAIL_ON_MISSING_CLASSES);
            if (getParameters().getIgnoreFailures().get() == false) {
                options.add(FAIL_ON_VIOLATION);
            }
            options.add(FAIL_ON_UNRESOLVABLE_SIGNATURES);
            options.add(DISABLE_CLASSLOADING_CACHE);
            final Checker checker = new Checker(new GradleForbiddenApiLogger(logger), urlLoader, options);
            return checker;
        }

        private static class GradleForbiddenApiLogger implements Logger {

            private final org.gradle.api.logging.Logger delegate;

            GradleForbiddenApiLogger(org.gradle.api.logging.Logger delegate) {
                this.delegate = delegate;
            }

            @Override
            public void error(String msg) {
                delegate.error(msg);
            }

            @Override
            public void warn(String msg) {
                delegate.warn(msg);
            }

            @Override
            public void info(String msg) {
                delegate.info(msg);
            }

            @Override
            public void debug(String msg) {
                delegate.debug(msg);
            }
        };
    }

    interface Parameters extends WorkParameters {
        ConfigurableFileCollection getClassDirectories();

        ConfigurableFileCollection getClassFiles();

        ConfigurableFileCollection getClasspath();

        SetProperty<String> getSuppressAnnotations();

        RegularFileProperty getSuccessMarker();

        ConfigurableFileCollection getSignaturesFiles();

        SetProperty<String> getBundledSignatures();

        Property<String> getTargetCompatibility();

        Property<Boolean> getIgnoreFailures();

        ListProperty<String> getSignatures();

    }

}
