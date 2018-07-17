package org.elasticsearch.gradle.precommit;

import groovy.lang.Closure;
import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.test.NamingConventionsCheck;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.SourceSetContainer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Objects;

/**
 * Runs NamingConventionsCheck on a classpath/directory combo to verify that
 * tests are named according to our conventions so they'll be picked up by
 * gradle. Read the Javadoc for NamingConventionsCheck to learn more.
 */
@SuppressWarnings("unchecked")
public class NamingConventionsTask extends LoggedExec {
    public NamingConventionsTask() {
        setDescription("Tests that test classes aren't misnamed or misplaced");
        final Project project = getProject();

        SourceSetContainer sourceSets = getJavaSourceSets();
        final FileCollection classpath = project.files(
            // This works because the class only depends on one class from junit that will be available from the
            // tests compile classpath. It's the most straight forward way of telling Java where to find the main
            // class.
            NamingConventionsCheck.class.getProtectionDomain().getCodeSource().getLocation().getPath(),
            // the tests to be loaded
            checkForTestsInMain ? sourceSets.getByName("main").getRuntimeClasspath() : project.files(),
            sourceSets.getByName("test").getCompileClasspath(),
            sourceSets.getByName("test").getOutput()
        );
        dependsOn(project.getTasks().matching(it -> "testCompileClasspath".equals(it.getName())));
        getInputs().files(classpath);

        setExecutable(new File(
            Objects.requireNonNull(
                project.getExtensions().getByType(ExtraPropertiesExtension.class).get("runtimeJavaHome")
            ).toString(),
            "bin/java")
        );

        if (checkForTestsInMain == false) {
            /* This task is created by default for all subprojects with this
             * setting and there is no point in running it if the files don't
             * exist. */
            onlyIf((unused) -> getExistingClassesDirs().isEmpty() == false);
        }

        /*
         * We build the arguments in a funny afterEvaluate/doFirst closure so that we can wait for the classpath to be
         * ready for us. Strangely neither one on their own are good enough.
         */
        project.afterEvaluate(new Closure<Void>(this, this) {
            public void doCall(Project it) {
                doFirst(unused -> {
                    args("-Djna.nosys=true");
                    args("-cp", classpath.getAsPath(), "org.elasticsearch.test.NamingConventionsCheck");
                    args("--test-class", getTestClass());
                    if (skipIntegTestInDisguise) {
                        args("--skip-integ-tests-in-disguise");
                    } else {
                        args("--integ-test-class", getIntegTestClass());
                    }
                    if (getCheckForTestsInMain()) {
                        args("--main");
                        args("--");
                    } else {
                        args("--");
                    }
                    args(getExistingClassesDirs().getAsPath());
                });
            }
        });
        doLast((Task it) -> {
            try {
                try (FileWriter fw = new FileWriter(getSuccessMarker())) {
                    fw.write("");
                }
            } catch (IOException e) {
                throw new GradleException("io exception", e);
            }
        });
    }

    private SourceSetContainer getJavaSourceSets() {
        return getProject().getConvention().getPlugin(JavaPluginConvention.class).getSourceSets();
    }

    public FileCollection getExistingClassesDirs() {
        FileCollection classesDirs = getJavaSourceSets().getByName(checkForTestsInMain ? "main" : "test")
            .getOutput().getClassesDirs();
        return classesDirs.filter(it -> it.exists());
    }

    public File getSuccessMarker() {
        return successMarker;
    }

    public void setSuccessMarker(File successMarker) {
        this.successMarker = successMarker;
    }

    public boolean getSkipIntegTestInDisguise() {
        return skipIntegTestInDisguise;
    }

    public boolean isSkipIntegTestInDisguise() {
        return skipIntegTestInDisguise;
    }

    public void setSkipIntegTestInDisguise(boolean skipIntegTestInDisguise) {
        this.skipIntegTestInDisguise = skipIntegTestInDisguise;
    }

    public String getTestClass() {
        return testClass;
    }

    public void setTestClass(String testClass) {
        this.testClass = testClass;
    }

    public String getIntegTestClass() {
        return integTestClass;
    }

    public void setIntegTestClass(String integTestClass) {
        this.integTestClass = integTestClass;
    }

    public boolean getCheckForTestsInMain() {
        return checkForTestsInMain;
    }

    public boolean isCheckForTestsInMain() {
        return checkForTestsInMain;
    }

    public void setCheckForTestsInMain(boolean checkForTestsInMain) {
        this.checkForTestsInMain = checkForTestsInMain;
    }

    /**
     * We use a simple "marker" file that we touch when the task succeeds
     * as the task output. This is compared against the modified time of the
     * inputs (ie the jars/class files).
     */
    @OutputFile
    private File successMarker = new File(getProject().getBuildDir(), "markers/" + this.getName());
    /**
     * Should we skip the integ tests in disguise tests? Defaults to true because only core names its
     * integ tests correctly.
     */
    @Input
    private boolean skipIntegTestInDisguise = false;
    /**
     * Superclass for all tests.
     */
    @Input
    private String testClass = "org.apache.lucene.util.LuceneTestCase";
    /**
     * Superclass for all integration tests.
     */
    @Input
    private String integTestClass = "org.elasticsearch.test.ESIntegTestCase";
    /**
     * Should the test also check the main classpath for test classes instead of
     * doing the usual checks to the test classpath.
     */
    @Input
    private boolean checkForTestsInMain = false;
}
