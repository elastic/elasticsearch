import jetbrains.buildServer.configs.kotlin.v2019_2.DslContext
import jetbrains.buildServer.configs.kotlin.v2019_2.project
import jetbrains.buildServer.configs.kotlin.v2019_2.vcs.GitVcsRoot
import jetbrains.buildServer.configs.kotlin.v2019_2.version

version = "2020.1"

val developmentBranches = listOf("master", "7.x", "7.8", "6.8")
val projectName = DslContext.getParameter("projectName", "UNKNOWN")

project {
    subProjectsOrder = developmentBranches.map { devBranch ->
        subProject {
            id(devBranch.replace('.', '_'))
            name = devBranch

            vcsRoot(GitVcsRoot {
                id("${projectName}_${devBranch.replace('.', '_')}")

                name = "${projectName} ($devBranch)"
                url = "https://github.com/elastic/${projectName.toLowerCase()}.git"
                branch = "refs/heads/$devBranch"
            })
        }
    }.map { it.id!! }
}
