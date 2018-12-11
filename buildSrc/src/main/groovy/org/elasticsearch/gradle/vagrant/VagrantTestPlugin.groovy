package org.elasticsearch.gradle.vagrant

import org.apache.tools.ant.taskdefs.condition.Os
import org.elasticsearch.gradle.FileContentsTask
import org.elasticsearch.gradle.LoggedExec
import org.elasticsearch.gradle.Version
import org.gradle.api.*
import org.gradle.api.artifacts.dsl.RepositoryHandler
import org.gradle.api.execution.TaskExecutionAdapter
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependency
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.StopExecutionException
import org.gradle.api.tasks.TaskState

import static java.util.Collections.unmodifiableList

class VagrantTestPlugin implements Plugin<Project> {

    /** All Linux boxes that we test. These are all always supplied **/
    static final List<String> LINUX_BOXES = unmodifiableList([
            'centos-6',
            'centos-7',
            'debian-8',
            'debian-9',
            'fedora-27',
            'fedora-28',
            'oel-6',
            'oel-7',
            'opensuse-42',
            'sles-12',
            'ubuntu-1404',
            'ubuntu-1604',
            'ubuntu-1804'
    ])

    /** All Windows boxes that we test, which may or may not be supplied **/
    static final List<String> WINDOWS_BOXES = unmodifiableList([
            'windows-2012r2',
            'windows-2016'
    ])

    /** All boxes that we test, some of which may not be supplied **/
    static final List<String> ALL_BOXES = unmodifiableList(LINUX_BOXES + WINDOWS_BOXES)

    /** Boxes used when sampling the tests **/
    static final List<String> SAMPLE = unmodifiableList([
            'centos-7',
            'ubuntu-1404'
    ])

    /** All distributions to bring into test VM, whether or not they are used **/
    static final List<String> DISTRIBUTIONS = unmodifiableList([
            'archives:tar',
            'archives:oss-tar',
            'archives:zip',
            'archives:oss-zip',
            'packages:rpm',
            'packages:oss-rpm',
            'packages:deb',
            'packages:oss-deb'
    ])

    /** Packages onboarded for upgrade tests **/
    static final List<String> UPGRADE_FROM_ARCHIVES = unmodifiableList(['rpm', 'deb'])

    private static final PACKAGING_CONFIGURATION = 'packaging'
    private static final PACKAGING_TEST_CONFIGURATION = 'packagingTest'
    private static final BATS = 'bats'
    private static final String BATS_TEST_COMMAND ="cd \$PACKAGING_ARCHIVES && sudo bats --tap \$BATS_TESTS/*.$BATS"
    private static final String PLATFORM_TEST_COMMAND ="rm -rf ~/elasticsearch && rsync -r /elasticsearch/ ~/elasticsearch && cd ~/elasticsearch && ./gradlew test integTest"

    /** Boxes that have been supplied and are available for testing **/
    List<String> availableBoxes = []

    /** extra env vars to pass to vagrant for box configuration **/
    Map<String, String> vagrantBoxEnvVars = [:]

    @Override
    void apply(Project project) {

        collectAvailableBoxes(project)

        // Creates the Vagrant extension for the project
        project.extensions.create('esvagrant', VagrantPropertiesExtension, listSelectedBoxes(project))

        // Add required repositories for packaging tests
        configurePackagingArchiveRepositories(project)

        // Creates custom configurations for Bats testing files (and associated scripts and archives)
        createPackagingConfiguration(project)
        project.configurations.create(PACKAGING_TEST_CONFIGURATION)

        // Creates all the main Vagrant tasks
        createVagrantTasks(project)

        if (project.extensions.esvagrant.boxes == null || project.extensions.esvagrant.boxes.size() == 0) {
            throw new InvalidUserDataException('Must specify at least one vagrant box')
        }

        for (String box : project.extensions.esvagrant.boxes) {
            if (ALL_BOXES.contains(box) == false) {
                throw new InvalidUserDataException("Vagrant box [${box}] is unknown to this plugin. Valid boxes are ${ALL_BOXES}")
            }

            if (availableBoxes.contains(box) == false) {
                throw new InvalidUserDataException("Vagrant box [${box}] is not available because an image is not supplied for it. " +
                    "Available boxes with supplied images are ${availableBoxes}")
            }
        }

        // Creates all tasks related to the Vagrant boxes
        createVagrantBoxesTasks(project)
    }

    /**
     * Enumerate all the boxes that we know about and could possibly choose to test
     */
    private void collectAvailableBoxes(Project project) {
        // these images are hardcoded in the Vagrantfile and are always available
        availableBoxes.addAll(LINUX_BOXES)

        // these images need to be provided at runtime
        String windows_2012r2_box = project.getProperties().get('vagrant.windows-2012r2.id')
        if (windows_2012r2_box != null && windows_2012r2_box.isEmpty() == false) {
            availableBoxes.add('windows-2012r2')
            vagrantBoxEnvVars['VAGRANT_WINDOWS_2012R2_BOX'] = windows_2012r2_box
        }

        String windows_2016_box = project.getProperties().get('vagrant.windows-2016.id')
        if (windows_2016_box != null && windows_2016_box.isEmpty() == false) {
            availableBoxes.add('windows-2016')
            vagrantBoxEnvVars['VAGRANT_WINDOWS_2016_BOX'] = windows_2016_box
        }
    }

    /**
     * Enumerate all the boxes that we have chosen to test
     */
    private static List<String> listSelectedBoxes(Project project) {
        String vagrantBoxes = project.getProperties().get('vagrant.boxes', 'sample')
        switch (vagrantBoxes) {
            case 'sample':
                return SAMPLE
            case 'linux-all':
                return LINUX_BOXES
            case 'windows-all':
                return WINDOWS_BOXES
            case 'all':
                return ALL_BOXES
            case '':
                return []
            default:
                return vagrantBoxes.split(',')
        }
    }

    private static void configurePackagingArchiveRepositories(Project project) {
        RepositoryHandler repos = project.repositories

        repos.jcenter() // will have releases before 5.0.0

        /* Setup a repository that tries to download from
          https://artifacts.elastic.co/downloads/elasticsearch/[module]-[revision].[ext]
          which should work for 5.0.0+. This isn't a real ivy repository but gradle
          is fine with that */
        repos.ivy {
            artifactPattern "https://artifacts.elastic.co/downloads/elasticsearch/[module]-[revision].[ext]"
        }
    }

    private static void createPackagingConfiguration(Project project) {
        project.configurations.create(PACKAGING_CONFIGURATION)

        String upgradeFromVersionRaw = System.getProperty("tests.packaging.upgradeVersion");
        Version upgradeFromVersion
        if (upgradeFromVersionRaw == null) {
            String firstPartOfSeed = project.rootProject.testSeed.tokenize(':').get(0)
            final long seed = Long.parseUnsignedLong(firstPartOfSeed, 16)
            final def indexCompatVersions = project.bwcVersions.indexCompatible
            upgradeFromVersion = indexCompatVersions[new Random(seed).nextInt(indexCompatVersions.size())]
        } else {
            upgradeFromVersion = Version.fromString(upgradeFromVersionRaw)
        }

        DISTRIBUTIONS.each {
            // Adds a dependency for the current version
            project.dependencies.add(PACKAGING_CONFIGURATION,
                    project.dependencies.project(path: ":distribution:${it}", configuration: 'default'))
        }

        UPGRADE_FROM_ARCHIVES.each {
            // The version of elasticsearch that we upgrade *from*
            project.dependencies.add(PACKAGING_CONFIGURATION,
                    "org.elasticsearch.distribution.${it}:elasticsearch:${upgradeFromVersion}@${it}")
            if (upgradeFromVersion.onOrAfter('6.3.0')) {
                project.dependencies.add(PACKAGING_CONFIGURATION,
                        "org.elasticsearch.distribution.${it}:elasticsearch-oss:${upgradeFromVersion}@${it}")
            }
        }

        project.extensions.esvagrant.upgradeFromVersion = upgradeFromVersion
    }

    private static void createCleanTask(Project project) {
        if (project.tasks.findByName('clean') == null) {
            project.tasks.create('clean', Delete.class) {
                description 'Clean the project build directory'
                group 'Build'
                delete project.buildDir
            }
        }
    }

    private static void createStopTask(Project project) {
        project.tasks.create('stop') {
            description 'Stop any tasks from tests that still may be running'
            group 'Verification'
        }
    }

    private static void createSmokeTestTask(Project project) {
        project.tasks.create('vagrantSmokeTest') {
            description 'Smoke test the specified vagrant boxes'
            group 'Verification'
        }
    }

    private static void createPrepareVagrantTestEnvTask(Project project) {
        File packagingDir = new File(project.buildDir, PACKAGING_CONFIGURATION)

        File archivesDir = new File(packagingDir, 'archives')
        Copy copyPackagingArchives = project.tasks.create('copyPackagingArchives', Copy) {
            into archivesDir
            from project.configurations[PACKAGING_CONFIGURATION]
        }

        File testsDir = new File(packagingDir, 'tests')
        Copy copyPackagingTests = project.tasks.create('copyPackagingTests', Copy) {
            into testsDir
            from project.configurations[PACKAGING_TEST_CONFIGURATION]
        }

        Task createLinuxRunnerScript = project.tasks.create('createLinuxRunnerScript', FileContentsTask) {
            dependsOn copyPackagingTests
            file "${testsDir}/run-tests.sh"
            contents """\
                     if [ "\$#" -eq 0 ]; then
                       test_args=( "${-> project.extensions.esvagrant.testClass}" )
                     else
                       test_args=( "\$@" )
                     fi
                     java -cp "\$PACKAGING_TESTS/*" org.elasticsearch.packaging.VMTestRunner "\${test_args[@]}"
                     """
        }
        Task createWindowsRunnerScript = project.tasks.create('createWindowsRunnerScript', FileContentsTask) {
            dependsOn copyPackagingTests
            file "${testsDir}/run-tests.ps1"
            // the use of $args rather than param() here is deliberate because the syntax for array (multivalued) parameters is likely
            // a little trappy for those unfamiliar with powershell
            contents """\
                     if (\$args.Count -eq 0) {
                       \$testArgs = @("${-> project.extensions.esvagrant.testClass}")
                     } else {
                       \$testArgs = \$args
                     }
                     java -cp "\$Env:PACKAGING_TESTS/*" org.elasticsearch.packaging.VMTestRunner @testArgs
                     exit \$LASTEXITCODE
                     """
        }

        Task createVersionFile = project.tasks.create('createVersionFile', FileContentsTask) {
            dependsOn copyPackagingArchives
            file "${archivesDir}/version"
            contents project.version
        }

        Task createUpgradeFromFile = project.tasks.create('createUpgradeFromFile', FileContentsTask) {
            String version = project.extensions.esvagrant.upgradeFromVersion
            if (project.bwcVersions.unreleased.contains(project.extensions.esvagrant.upgradeFromVersion)) {
                version += "-SNAPSHOT"
            }
            dependsOn copyPackagingArchives
            file "${archivesDir}/upgrade_from_version"
            contents version
        }

        Task createUpgradeIsOssFile = project.tasks.create('createUpgradeIsOssFile', FileContentsTask) {
            dependsOn copyPackagingArchives
            doFirst {
                project.delete("${archivesDir}/upgrade_is_oss")
                if (project.extensions.esvagrant.upgradeFromVersion.before('6.3.0')) {
                    throw new StopExecutionException("upgrade version is before 6.3.0")
                }
            }
            file "${archivesDir}/upgrade_is_oss"
            contents ''
        }

        File batsDir = new File(packagingDir, BATS)
        Copy copyBatsTests = project.tasks.create('copyBatsTests', Copy) {
            into "${batsDir}/tests"
            from {
                "${project.extensions.esvagrant.batsDir}/tests"
            }
        }

        Copy copyBatsUtils = project.tasks.create('copyBatsUtils', Copy) {
            into "${batsDir}/utils"
            from {
                "${project.extensions.esvagrant.batsDir}/utils"
            }
        }

        // Now we iterate over dependencies of the bats configuration. When a project dependency is found,
        // we bring back its test files or test utils.
        project.afterEvaluate {
            project.configurations[PACKAGING_CONFIGURATION].dependencies
                .findAll {it.targetConfiguration == PACKAGING_CONFIGURATION }
                .each { d ->
                    if (d instanceof DefaultProjectDependency) {
                        DefaultProjectDependency externalBatsDependency = (DefaultProjectDependency) d
                        Project externalBatsProject = externalBatsDependency.dependencyProject
                        String externalBatsDir = externalBatsProject.extensions.esvagrant.batsDir

                        if (project.extensions.esvagrant.inheritTests) {
                            copyBatsTests.from(externalBatsProject.files("${externalBatsDir}/tests"))
                        }
                        if (project.extensions.esvagrant.inheritTestUtils) {
                            copyBatsUtils.from(externalBatsProject.files("${externalBatsDir}/utils"))
                        }
                    }
            }
        }

        Task vagrantSetUpTask = project.tasks.create('setupPackagingTest')
        vagrantSetUpTask.dependsOn(
            'vagrantCheckVersion',
            copyPackagingArchives,
            copyPackagingTests,
            createLinuxRunnerScript,
            createWindowsRunnerScript,
            createVersionFile,
            createUpgradeFromFile,
            createUpgradeIsOssFile,
            copyBatsTests,
            copyBatsUtils
        )
    }

    private static void createPackagingTestTask(Project project) {
        project.tasks.create('packagingTest') {
            group 'Verification'
            description "Tests distribution installation on different platforms using vagrant. See TESTING.asciidoc for details."
            dependsOn 'vagrantCheckVersion'
        }
    }

    private static void createPlatformTestTask(Project project) {
        project.tasks.create('platformTest') {
            group 'Verification'
            description "Test unit and integ tests on different platforms using vagrant. See TESTING.asciidoc for details. This test " +
                    "is unmaintained."
            dependsOn 'vagrantCheckVersion'
        }
    }

    private void createBoxListTasks(Project project) {
        project.tasks.create('listAllBoxes') {
            group 'Verification'
            description 'List all vagrant boxes which can be tested by this plugin'
            doLast {
                println("All vagrant boxes supported by ${project.path}")
                for (String box : ALL_BOXES) {
                    println(box)
                }
            }
            dependsOn 'vagrantCheckVersion'
        }

        project.tasks.create('listAvailableBoxes') {
            group 'Verification'
            description 'List all vagrant boxes which are available for testing'
            doLast {
                println("All vagrant boxes available to ${project.path}")
                for (String box : availableBoxes) {
                    println(box)
                }
            }
            dependsOn 'vagrantCheckVersion'
        }
    }

    private void createVagrantTasks(Project project) {
        createCleanTask(project)
        createStopTask(project)
        createSmokeTestTask(project)
        createPrepareVagrantTestEnvTask(project)
        createPackagingTestTask(project)
        createPlatformTestTask(project)
        createBoxListTasks(project)
    }

    private void createVagrantBoxesTasks(Project project) {
        assert project.extensions.esvagrant.boxes != null

        assert project.tasks.stop != null
        Task stop = project.tasks.stop

        assert project.tasks.vagrantSmokeTest != null
        Task vagrantSmokeTest = project.tasks.vagrantSmokeTest

        assert project.tasks.vagrantCheckVersion != null
        Task vagrantCheckVersion = project.tasks.vagrantCheckVersion

        assert project.tasks.virtualboxCheckVersion != null
        Task virtualboxCheckVersion = project.tasks.virtualboxCheckVersion

        assert project.tasks.setupPackagingTest != null
        Task setupPackagingTest = project.tasks.setupPackagingTest

        assert project.tasks.packagingTest != null
        Task packagingTest = project.tasks.packagingTest

        assert project.tasks.platformTest != null
        Task platformTest = project.tasks.platformTest

        /*
         * We always use the main project.rootDir as Vagrant's current working directory (VAGRANT_CWD)
         * so that boxes are not duplicated for every Gradle project that use this VagrantTestPlugin.
         */
        def vagrantEnvVars = [
                'VAGRANT_CWD'           : "${project.rootDir.absolutePath}",
                'VAGRANT_VAGRANTFILE'   : 'Vagrantfile',
                'VAGRANT_PROJECT_DIR'   : "${project.projectDir.absolutePath}"
        ]
        vagrantEnvVars.putAll(vagrantBoxEnvVars)

        // Each box gets it own set of tasks
        for (String box : availableBoxes) {
            String boxTask = box.capitalize().replace('-', '')

            // always add a halt task for all boxes, so clean makes sure they are all shutdown
            Task halt = project.tasks.create("vagrant${boxTask}#halt", VagrantCommandTask) {
                command 'halt'
                boxName box
                environmentVars vagrantEnvVars
            }
            stop.dependsOn(halt)

            Task update = project.tasks.create("vagrant${boxTask}#update", VagrantCommandTask) {
                command 'box'
                subcommand 'update'
                boxName box
                environmentVars vagrantEnvVars
                dependsOn vagrantCheckVersion, virtualboxCheckVersion
            }
            update.mustRunAfter(setupPackagingTest)

            /*
             * Destroying before every execution can be annoying while iterating on tests locally. Therefore, we provide a flag
             * vagrant.destroy that defaults to true that can be used to control whether or not to destroy any test boxes before test
             * execution.
             */
            final String vagrantDestroyProperty = project.getProperties().get('vagrant.destroy', 'true')
            boolean vagrantDestroy
            if ("true".equals(vagrantDestroyProperty)) {
                vagrantDestroy = true
            } else if ("false".equals(vagrantDestroyProperty)) {
                vagrantDestroy = false
            } else {
                throw new GradleException("[vagrant.destroy] must be [true] or [false] but was [" + vagrantDestroyProperty + "]")
            }
            /*
             * Some versions of Vagrant will fail destroy if the box does not exist. Therefore we check if the box exists before attempting
             * to destroy the box.
             */
            final Task destroy = project.tasks.create("vagrant${boxTask}#destroy", LoggedExec) {
                commandLine "bash", "-c", "vagrant status ${box} | grep -q \"${box}\\s\\+not created\" || vagrant destroy ${box} --force"
                workingDir project.rootProject.rootDir
                environment vagrantEnvVars
            }
            destroy.onlyIf { vagrantDestroy }
            update.mustRunAfter(destroy)

            Task up = project.tasks.create("vagrant${boxTask}#up", VagrantCommandTask) {
                command 'up'
                boxName box
                environmentVars vagrantEnvVars
                /* We lock the provider to virtualbox because the Vagrantfile specifies
                  lots of boxes that only work properly in virtualbox. Virtualbox is
                  vagrant's default but its possible to change that default and folks do.
                  But the boxes that we use are unlikely to work properly with other
                  virtualization providers. Thus the lock. */
                args '--provision', '--provider', 'virtualbox'
                /* It'd be possible to check if the box is already up here and output
                  SKIPPED but that would require running vagrant status which is slow! */
                dependsOn destroy, update
            }

            Task smoke = project.tasks.create("vagrant${boxTask}#smoketest", Exec) {
                environment vagrantEnvVars
                dependsOn up
                finalizedBy halt
            }
            vagrantSmokeTest.dependsOn(smoke)
            if (LINUX_BOXES.contains(box)) {
                smoke.commandLine = ['vagrant', 'ssh', box, '--command',
                    "set -o pipefail && echo 'Hello from ${project.path}' | sed -ue 's/^/    ${box}: /'"]
            } else {
                smoke.commandLine = ['vagrant', 'winrm', box, '--command',
                    "Write-Host '    ${box}: Hello from ${project.path}'"]
            }

            if (LINUX_BOXES.contains(box)) {
                Task batsPackagingTest = project.tasks.create("vagrant${boxTask}#batsPackagingTest", BatsOverVagrantTask) {
                    remoteCommand BATS_TEST_COMMAND
                    boxName box
                    environmentVars vagrantEnvVars
                    dependsOn up, setupPackagingTest
                    finalizedBy halt
                }

                TaskExecutionAdapter batsPackagingReproListener = createReproListener(project, batsPackagingTest.path)
                batsPackagingTest.doFirst {
                    project.gradle.addListener(batsPackagingReproListener)
                }
                batsPackagingTest.doLast {
                    project.gradle.removeListener(batsPackagingReproListener)
                }
                if (project.extensions.esvagrant.boxes.contains(box)) {
                    // these tests are temporarily disabled for suse boxes while we debug an issue
                    // https://github.com/elastic/elasticsearch/issues/30295
                    if (box.equals("opensuse-42") == false && box.equals("sles-12") == false) {
                        packagingTest.dependsOn(batsPackagingTest)
                    }
                }
            }

            Task javaPackagingTest = project.tasks.create("vagrant${boxTask}#javaPackagingTest", VagrantCommandTask) {
                boxName box
                environmentVars vagrantEnvVars
                dependsOn up, setupPackagingTest
                finalizedBy halt
            }

            // todo remove this onlyIf after all packaging tests are consolidated
            javaPackagingTest.onlyIf {
                project.extensions.esvagrant.testClass != null
            }

            if (LINUX_BOXES.contains(box)) {
                javaPackagingTest.command = 'ssh'
                javaPackagingTest.args = ['--command', 'sudo bash "$PACKAGING_TESTS/run-tests.sh"']
            } else {
                // powershell sessions run over winrm always run as administrator, whether --elevated is passed or not. however
                // remote sessions have some restrictions on what they can do, such as impersonating another user (or the same user
                // without administrator elevation), which we need to do for these tests. passing --elevated runs the session
                // as a scheduled job locally on the vm as a true administrator to get around this limitation
                //
                // https://github.com/hashicorp/vagrant/blob/9c299a2a357fcf87f356bb9d56e18a037a53d138/plugins/communicators/winrm/communicator.rb#L195-L225
                // https://devops-collective-inc.gitbooks.io/secrets-of-powershell-remoting/content/manuscript/accessing-remote-computers.html
                javaPackagingTest.command = 'winrm'
                javaPackagingTest.args = ['--elevated', '--command', 'powershell -File "$Env:PACKAGING_TESTS/run-tests.ps1"']
            }

            TaskExecutionAdapter javaPackagingReproListener = createReproListener(project, javaPackagingTest.path)
            javaPackagingTest.doFirst {
                project.gradle.addListener(javaPackagingReproListener)
            }
            javaPackagingTest.doLast {
                project.gradle.removeListener(javaPackagingReproListener)
            }
            if (project.extensions.esvagrant.boxes.contains(box)) {
                // these tests are temporarily disabled for suse boxes while we debug an issue
                // https://github.com/elastic/elasticsearch/issues/30295
                if (box.equals("opensuse-42") == false && box.equals("sles-12") == false) {
                    packagingTest.dependsOn(javaPackagingTest)
                }
            }

            /*
             * This test is unmaintained and was created to run on Linux. We won't allow it to run on Windows
             * until it's been brought back into maintenance
             */
            if (LINUX_BOXES.contains(box)) {
                Task platform = project.tasks.create("vagrant${boxTask}#platformTest", VagrantCommandTask) {
                    command 'ssh'
                    boxName box
                    environmentVars vagrantEnvVars
                    dependsOn up
                    finalizedBy halt
                    args '--command', PLATFORM_TEST_COMMAND + " -Dtests.seed=${-> project.testSeed}"
                }
                TaskExecutionAdapter platformReproListener = createReproListener(project, platform.path)
                platform.doFirst {
                    project.gradle.addListener(platformReproListener)
                }
                platform.doLast {
                    project.gradle.removeListener(platformReproListener)
                }
                if (project.extensions.esvagrant.boxes.contains(box)) {
                    platformTest.dependsOn(platform)
                }
            }
        }
    }

    private static TaskExecutionAdapter createReproListener(Project project, String reproTaskPath) {
        return new TaskExecutionAdapter() {
            @Override
            void afterExecute(Task task, TaskState state) {
                final String gradlew = Os.isFamily(Os.FAMILY_WINDOWS) ? "gradlew" : "./gradlew"
                if (state.failure != null) {
                    println "REPRODUCE WITH: ${gradlew} ${reproTaskPath} -Dtests.seed=${project.testSeed} "
                }
            }
        }
    }
}
