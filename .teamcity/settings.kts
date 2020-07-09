import jetbrains.buildServer.configs.kotlin.v2019_2.project
import jetbrains.buildServer.configs.kotlin.v2019_2.vcs.GitVcsRoot
import jetbrains.buildServer.configs.kotlin.v2019_2.version

version = "2020.1"

val developmentBranches = listOf("6.8", "7.8", "7.x", "master")

project {
    developmentBranches.forEach { devBranch ->
        subProject {
            id(devBranch.replace('.', '_'))
            name = devBranch

            vcsRoot(GitVcsRoot {
                id("Elasticsearch_${devBranch.replace('.', '_')}")

                name = "Elasticsearch ($devBranch)"
                url = "https://github.com/elastic/elasticsearch.git"
                branch = "refs/heads/$devBranch"
            })
        }
    }
}
