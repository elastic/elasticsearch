package org.elasticsearch.gradle;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Files.write;
import static java.util.Collections.singletonMap;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.internal.impldep.org.junit.rules.ExpectedException;
import org.gradle.internal.impldep.org.junit.rules.TemporaryFolder;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;

public class NoticeTaskTests extends GradleUnitTestCase {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Project project;
    private NoticeTask task;

    private NoticeTask createTask(Project project) {
        return (NoticeTask) project.task(singletonMap("type", NoticeTask.class), "noticeTask");
    }

    private Project createProject() throws IOException {
        temporaryFolder.create();
        File projectDir = temporaryFolder.newFolder();
        return ProjectBuilder.builder().withProjectDir(projectDir).build();
    }


    public void testGenerateAllNotices() throws Exception {
        project = createProject();
        task = createTask(project);
        Path projectDir = project.getProjectDir().toPath();
        createFileIn(projectDir, "NOTICE.txt", "This is\nthe root notice\n");
        Path licenseDir = projectDir.resolve("licenseDir");
        createFileIn(licenseDir, "myNotice-NOTICE.txt", "This is\nmy notice\n");
        createFileIn(licenseDir, "myNotice-LICENSE.txt", "This is\nmy license\n");
        task.setLicensesDir(licenseDir.toFile());

        task.generateNotice();

        Path notice = project.getBuildDir().toPath().resolve("notices").resolve(task.getName()).resolve("NOTICE.txt");
        String generatedContent = new String(readAllBytes(notice), UTF8);
        String expected = "This is\n" +
            "the root notice\n" +
            "\n" +
            "\n" +
            "================================================================================\n" +
            "myNotice NOTICE\n" +
            "================================================================================\n" +
            "This is\n" +
            "my notice\n" +
            "\n" +
            "\n" +
            "================================================================================\n" +
            "myNotice LICENSE\n" +
            "================================================================================\n" +
            "This is\n" +
            "my license";
        assertEquals(expected, generatedContent.trim());
    }

    public void testShouldFailWhenMultipleNoticeIsPresent() throws Exception {
        project = createProject();
        task = createTask(project);
        Path projectDir = project.getProjectDir().toPath();
        createFileIn(projectDir, "NOTICE.txt", "This is\nthe root notice\n");
        Path licenseDir = projectDir.resolve("licenseDir");
        createFileIn(licenseDir, "myNotice1-NOTICE.txt", "This is\nmy notice\n");
        createFileIn(licenseDir, "myNotice1-LICENSE.txt", "This is\nmy notice\n");
        createFileIn(licenseDir, "myNotice2-NOTICE.txt", "This is\nmy notice\n");
        createFileIn(licenseDir, "myNotice2-LICENSE.txt", "This is\nmy notice\n");
        task.setLicensesDir(licenseDir.toFile());

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Two different notices exist");

        task.generateNotice();
    }

    private void createFileIn(Path parent, String name, String content) throws IOException {
        parent.toFile().mkdir();
        Path file = parent.resolve(name);
        file.toFile().createNewFile();
        write(file, content.getBytes(UTF8));
    }
}
