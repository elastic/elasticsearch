package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.test.NamingConventionsCheck;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Runs NamingConventionsCheck on a classpath/directory combo to verify that
 * tests are named according to our conventions so they'll be picked up by
 * gradle. Read the Javadoc for NamingConventionsCheck to learn more.
 */
@SuppressWarnings("unchecked")
public class NamingConventionsTask extends PrecommitTask {

    public NamingConventionsTask() {
        setDescription("Tests that test classes aren't misnamed or misplaced");
        dependsOn(getJavaSourceSets().getByName(checkForTestsInMain ? "main" : "test").getClassesTaskName());
    }

    @TaskAction
    public void runNamingConventions() {
        LoggedExec.javaexec(getProject(), spec -> {
            spec.classpath(
                getNamingConventionsCheckClassFiles(),
                getSourceSetClassPath()
            );
            spec.executable(getJavaHome() + "/bin/java");
            spec.jvmArgs("-Djna.nosys=true");
            spec.setMain(NamingConventionsCheck.class.getName());
            spec.args("--test-class", getTestClass());
            if (isSkipIntegTestInDisguise()) {
                spec.args("--skip-integ-tests-in-disguise");
            } else {
                spec.args("--integ-test-class", getIntegTestClass());
            }
            if (isCheckForTestsInMain()) {
                spec.args("--main");
                spec.args("--");
            } else {
                spec.args("--");
            }
            spec.args(getExistingClassesDirs().getAsPath());
        });
    }

    @Input
    public Object getJavaHome() {
        return javaHome;
    }

    public void setJavaHome(Object javaHome) {
        this.javaHome = javaHome;
    }

    @Classpath
    public FileCollection getSourceSetClassPath() {
        SourceSetContainer sourceSets = getJavaSourceSets();
        return getProject().files(
            sourceSets.getByName("test").getCompileClasspath(),
            sourceSets.getByName("test").getOutput(),
            checkForTestsInMain ? sourceSets.getByName("main").getRuntimeClasspath() : getProject().files()
        );
    }

    @InputFiles
    public File getNamingConventionsCheckClassFiles() {
        // This works because the class only depends on one class from junit that will be available from the
        // tests compile classpath. It's the most straight forward way of telling Java where to find the main
        // class.
        URL location = NamingConventionsCheck.class.getProtectionDomain().getCodeSource().getLocation();
        if (location.getProtocol().equals("file") == false) {
            throw new GradleException("Unexpected location for NamingConventionCheck class: "+ location);
        }
        try {
            return new File(location.toURI().getPath());
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
    }

    @InputFiles
    @SkipWhenEmpty
    public FileCollection getExistingClassesDirs() {
        FileCollection classesDirs = getJavaSourceSets().getByName(checkForTestsInMain ? "main" : "test")
            .getOutput().getClassesDirs();
        return classesDirs.filter(it -> it.exists());
    }

    @Input
    public boolean isSkipIntegTestInDisguise() {
        return skipIntegTestInDisguise;
    }

    public void setSkipIntegTestInDisguise(boolean skipIntegTestInDisguise) {
        this.skipIntegTestInDisguise = skipIntegTestInDisguise;
    }

    @Input
    public String getTestClass() {
        return testClass;
    }

    public void setTestClass(String testClass) {
        this.testClass = testClass;
    }

    @Input
    public String getIntegTestClass() {
        return integTestClass;
    }

    public void setIntegTestClass(String integTestClass) {
        this.integTestClass = integTestClass;
    }

    @Input
    public boolean isCheckForTestsInMain() {
        return checkForTestsInMain;
    }

    public void setCheckForTestsInMain(boolean checkForTestsInMain) {
        this.checkForTestsInMain = checkForTestsInMain;
    }

    private SourceSetContainer getJavaSourceSets() {
        return getProject().getConvention().getPlugin(JavaPluginConvention.class).getSourceSets();
    }

    /**
     * The java home to run the check with
     */
    private Object javaHome; // Make it an Object to allow for Groovy GString

    /**
     * Should we skip the integ tests in disguise tests? Defaults to true because only core names its
     * integ tests correctly.
     */
    private boolean skipIntegTestInDisguise = false;

    /**
     * Superclass for all tests.
     */
    private String testClass = "org.apache.lucene.util.LuceneTestCase";

    /**
     * Superclass for all integration tests.
     */
    private String integTestClass = "org.elasticsearch.test.ESIntegTestCase";

    /**
     * Should the test also check the main classpath for test classes instead of
     * doing the usual checks to the test classpath.
     */
    private boolean checkForTestsInMain = false;
}
