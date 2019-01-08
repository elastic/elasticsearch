package org.elasticsearch.gradle;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.junit.Rule;
import org.junit.Test;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.io.FileUtils.write;
import static org.apache.commons.io.FileUtils.readFileToString;

public class NoticeTaskTest extends GradleUnitTestCase {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();
    private NoticeTask noticeTask;
    private Project project;
    final String outputHeader = "This is the header for the output file\nIt should contain:\n3 lines & 2 spaces";

    public List<File> getListWithoutCopies() throws IOException {
        File directory1 = new File(noticeTask.getTemporaryDir(), "directoryA");
        File directory2 = new File(noticeTask.getTemporaryDir(), "directoryB");

        File d1Notice = new File(directory1, "test-NOTICE.txt");
        File d1License = new File(directory1, "test-LICENSE.txt");
        File d2Notice = new File(directory2, "test2-NOTICE.txt");
        File d2License = new File(directory2, "test2-LICENSE.txt");

        write(d1Notice, "d1 Notice text file");
        write(d2Notice, "d2 Notice text file");
        write(d1License, "d1 License text file");
        write(d2License, "d2 License text file");

        List<File> files = new ArrayList<>();

        files.add(d1License);
        files.add(d1Notice);
        files.add(d2License);
        files.add(d2Notice);

        return files;
    }

    public List<File> getListWithCopies() throws IOException {
        List<File> files = this.getListWithoutCopies();

        File directory2 = new File(noticeTask.getTemporaryDir(), "directoryC");
        File d1NoticeCopy = new File(directory2, "test-NOTICE.txt");
        File d1LicenseCopy = new File(directory2, "test-LICENSE.txt");

        write(d1NoticeCopy, "d1 Copy Notice text file");
        write(d1LicenseCopy, "d1 License text file");

        files.add(d1LicenseCopy);
        files.add(d1NoticeCopy);

        return files;
    }

    @Test
    public void verifyGenerateNoticeWithException() throws IOException {
        // Setup everything so we can test out the task
        project = createProject();
        noticeTask = createTask(project);
        File inputFile = new File(project.getProjectDir(), "NOTICE.txt");
        File outputFile = new File(project.getProjectDir(), "OUTPUT.txt");
        Files.write(inputFile.toPath(), this.outputHeader.getBytes());

        // Set the input and output files on the NoticeTask so we can compare them later
        noticeTask.setInputFile(inputFile);
        noticeTask.setOutputFile(outputFile);
        this.getListWithCopies().forEach(noticeTask::licensesDir);

        // Should see an exception because there will be different notices for the same item
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Two different notices exist for dependency");
        noticeTask.generateNotice();
    }

    @Test
    public void verifyGenerateNotice() throws IOException {
        // Setup everything so we can test out the task
        project = createProject();
        noticeTask = createTask(project);
        final File inputFile = new File(project.getProjectDir(), "NOTICE.txt");
        final File outputFile = new File(project.getProjectDir(), "OUTPUT.txt");

        // Set the input and output files on the NoticeTask so we can compare them later
        noticeTask.setInputFile(inputFile);
        noticeTask.setOutputFile(outputFile);

        // Give us some dummy data to work with
        Files.write(inputFile.toPath(), this.outputHeader.getBytes());
        this.getListWithoutCopies()
            .forEach(noticeTask::licensesDir);

        // Generate the notice output
        noticeTask.generateNotice();

        // Get the output String from the output file so we can compare it
        final String outputText = readFileToString(outputFile, "UTF-8");

        // We should be able to find all the text from each file in the output
        noticeTask.getLicensesDirs().forEach(file -> {
            try {
                String text = readFileToString(file, "UTF-8");
                assertTrue(outputText.contains(text));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        final String lineDivider =
            "================================================================================";

        // We shouldn't have any of the 'copy' notices
        final String d1NoticeCopy = "d1 Copy Notice text file";
        final String d1LicenseCopy = "d1 copy License text file";

        assertFalse(outputText.contains(d1LicenseCopy));
        assertFalse(outputText.contains(d1NoticeCopy));
        assertTrue(outputText.contains(lineDivider));
    }

    @Test
    public void EnsureDirectoriesAreLoaded(){
        project = createProject();
        noticeTask = createTask(project);
        noticeTask.licensesDir(new File(project.getProjectDir(), "NOTICE.txt"));

        var list = noticeTask.getLicensesDirs();
        assertTrue(list.get(0).toString().endsWith("NOTICE.txt"));
    }

    private Project createProject() {
        return ProjectBuilder.builder().build();
    }

    private NoticeTask createTask(Project project) {
        return project.getTasks().create("NoticeTask", NoticeTask.class);
    }
}
