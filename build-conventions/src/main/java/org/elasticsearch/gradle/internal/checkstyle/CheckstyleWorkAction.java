/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.checkstyle;

import com.puppycrawl.tools.checkstyle.*;
import com.puppycrawl.tools.checkstyle.api.AuditListener;
import com.puppycrawl.tools.checkstyle.api.AutomaticBean;
import com.puppycrawl.tools.checkstyle.api.CheckstyleException;
import com.puppycrawl.tools.checkstyle.api.Configuration;
import com.puppycrawl.tools.checkstyle.api.RootModule;
import com.puppycrawl.tools.checkstyle.api.Violation;
import org.apache.commons.io.FileUtils;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import picocli.CommandLine;

public abstract class CheckstyleWorkAction implements WorkAction<CheckstyleWorkParameters> {

    @Override
    public void execute() {
        try {
            List<File> sourceFiles = getParameters().getSourceFiles().get();
            File configFile = getParameters().getConfig().getAsFile().get();
            File outputFile = getParameters().getOutputFile().getAsFile().get();
            int errorCount = runCheckstyle(configFile, outputFile, sourceFiles);
            if (errorCount > 0) {
                throw new CheckstyleException("Checkstyle has encountered " + errorCount + " problem(s).");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int runCheckstyle(File configFile, File outputFile, List<File> filesToProcess) throws CheckstyleException, IOException {
        // setup the properties
        final Properties props = new Properties();
//        System.getProperties();

        props.put("config_loc", configFile.getParent());
        // create a configuration
        final ThreadModeSettings multiThreadModeSettings = new ThreadModeSettings(1, 1);

        final ConfigurationLoader.IgnoredModulesOptions ignoredModulesOptions = ConfigurationLoader.IgnoredModulesOptions.OMIT;

        final Configuration config = ConfigurationLoader.loadConfiguration(
            configFile.getAbsolutePath(),
            new PropertiesExpander(props),
            ignoredModulesOptions,
            multiThreadModeSettings
        );

        // create RootModule object and run it
        final int errorCounter;
        final ClassLoader moduleClassLoader = Checker.class.getClassLoader();
        final RootModule rootModule = getRootModule(config.getName(), moduleClassLoader);
        try {
            final AuditListener listener = createListener(outputFile.toPath());
            rootModule.setModuleClassLoader(moduleClassLoader);
            rootModule.configure(config);
            rootModule.addListener(listener);

            // run RootModule
            errorCounter = rootModule.process(filesToProcess);
        } finally {
            rootModule.destroy();
        }

        return errorCounter;
    }

    private static RootModule getRootModule(String name, ClassLoader moduleClassLoader) throws CheckstyleException {
        final ModuleFactory factory = new PackageObjectFactory(Checker.class.getPackage().getName(), moduleClassLoader);

        return (RootModule) factory.createModule(name);
    }

    private static AuditListener createListener(Path outputLocation) throws IOException {
        final OutputStream out = getOutputStream(outputLocation);
        final AutomaticBean.OutputStreamOptions closeOutputStreamOption = getOutputStreamOptions(outputLocation);
        return new XMLLogger(out, closeOutputStreamOption);
    }

    /**
     * Create output stream or return System.out
     *
     * @param outputPath output location
     * @return output stream
     * @throws IOException might happen
     * @noinspection UseOfSystemOutOrSystemErr
     */
    @SuppressWarnings("resource")
    private static OutputStream getOutputStream(Path outputPath) throws IOException {
        final OutputStream result;
        if (outputPath == null) {
            result = System.out;
        } else {
            result = Files.newOutputStream(outputPath);
        }
        return result;
    }

    /**
     * Create {@link AutomaticBean.OutputStreamOptions} for the given location.
     *
     * @param outputPath output location
     * @return output stream options
     */
    private static AutomaticBean.OutputStreamOptions getOutputStreamOptions(Path outputPath) {
        final AutomaticBean.OutputStreamOptions result;
        if (outputPath == null) {
            result = AutomaticBean.OutputStreamOptions.NONE;
        } else {
            result = AutomaticBean.OutputStreamOptions.CLOSE;
        }
        return result;
    }

}
