package org.elasticsearch.gradle;


import static java.util.Collections.singletonMap;
import static org.apache.commons.io.FileUtils.readFileToString;

import java.io.File;
import java.io.IOException;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class FileContentsTaskTests extends GradleUnitTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private FileContentsTask fileContentsTask;
    private Project project;

    public void testWriteFile() throws Exception {
        project = createProject();
        fileContentsTask = createTask(project);
        File targetFile = new File(project.getProjectDir(),"testFile");
        String expectedContent = "the expected content\nis here.";

        fileContentsTask.setFile(targetFile);
        fileContentsTask.setContents(expectedContent);
        fileContentsTask.writeFile();

        assertEquals(expectedContent, readFileToString(targetFile));
    }

    public void testWriteFileSpecifiedByString() throws Exception {
        project = createProject();
        fileContentsTask = createTask(project);
        File targetFile = new File(project.getProjectDir(),"testFile");
        String expectedContent = "the expected content\nis here.";

        fileContentsTask.setFile("testFile");
        fileContentsTask.setContents(expectedContent);
        fileContentsTask.writeFile();

        assertEquals(expectedContent, readFileToString(targetFile));
    }

    private Project createProject() throws IOException {
        return ProjectBuilder.builder().withProjectDir(temporaryFolder.newFolder()).build();
    }

    private FileContentsTask createTask(Project project) {
        return (FileContentsTask) project.task(singletonMap("type", FileContentsTask.class), "fileContentTask");
    }

}
