#!/bin/bash
set -e
set -u
set -o pipefail

# This script creates a new branch then migrate all the maven stuff in it and commit that branch.
# then it runs mvn install to check that everything is still running

# Useful commands while testing
# rm -rf migration/tmp plugins
# git checkout pr/migration_script -f
# git add .; git commit -m "Add migration script" --amend
# ./migrate.sh 
# mvn clean install -DskipTests

GIT_BRANCH="refactoring/add_lang"

# Insert a new text after a given line
# insertLinesAfter text_to_find text_to_add newLine_separator filename
# insertLinesAfter "<\/project>" "   <modules>=   <\/modules>" "§" "pom.xml"
function insertLinesAfter() {
	# echo "## modify $4 with $2"
	sed "s/$1/$1$3$2/" $4 | tr "$3" "\n" > $4.new
	mv $4.new $4
}

# Insert a new text before a given line
# insertLinesBefore text_to_find text_to_add newLine_separator filename
# insertLinesBefore "<\/project>" "   <modules>=   <\/modules>" "§" "pom.xml"
function insertLinesBefore() {
	# echo "## modify $4 with $2"
	sed "s/$1/$2$3$1/" $4 | tr "$3" "\n" > $4.new
	mv $4.new $4
}

# Replace text in a file
# replaceLine old_text new_text filename
# replaceLine "old" "new" "pom.xml"
function replaceLine() {
	# echo "## modify $3 with $2"
	sed "s/^$1/$2/" $3 > $3.new
	mv $3.new $3
}

# Remove lines in a file
# removeLines from_text_included to_text_included filename
# removeLines "old" "new" "pom.xml"
function removeLines() {
	# echo "## remove lines in $3 from $1 to $2"
	sed "/$1/,/$2/d" $3 > $3.new
	mv $3.new $3
}

# Migrate a plugin
# migratePlugin short_name
function migratePlugin() {
	PLUGIN_GIT_REPO="https://github.com/elastic/elasticsearch-$1.git"
	git remote add remote_$1 $PLUGIN_GIT_REPO
	git fetch remote_$1 > /dev/null 2>/dev/null
	echo "## Add $1 module from $PLUGIN_GIT_REPO"

	# echo "### fetch $1 project from $PLUGIN_GIT_REPO in plugins"
	# If you want to run that locally, uncomment this line and comment one below
	#cp -R ../elasticsearch-$1/* plugins/$1
	# git clone $PLUGIN_GIT_REPO plugins/$1 > /dev/null 2>/dev/null
	git checkout -b migrate_$1 remote_$1/master > /dev/null 2>/dev/null
	git remote rm remote_$1 > /dev/null 2>/dev/null
	mkdir -p plugins/$1
	git mv -k * plugins/$1 > /dev/null 2>/dev/null       
	git rm .gitignore > /dev/null 2>/dev/null
	# echo "### change $1 groupId to org.elasticsearch.plugin"
	# Change the groupId to avoid conflicts with existing 2.0.0 versions.
	replaceLine "    <groupId>org.elasticsearch<\/groupId>" "    <groupId>org.elasticsearch.plugin<\/groupId>" "plugins/$1/pom.xml"

	# echo "### cleanup $1 pom.xml"
	removeLines "<issueManagement>" "<\/issueManagement>" "plugins/$1/pom.xml"
	removeLines "<repositories>" "<\/repositories>" "plugins/$1/pom.xml"
	removeLines "<url>" "<\/scm>" "plugins/$1/pom.xml"

        # echo "### remove version 5.0.0-SNAPSHOT from $1 pom.xml"
        # All plugins for ES 5.0.0 uses 5.0.0-SNAPSHOT version number
        replaceLine "    <version>5.0.0-SNAPSHOT<\/version>" "" "plugins/$1/pom.xml"

	# echo "### remove unused dev-tools and .git dirs and LICENSE.txt and CONTRIBUTING.md files"
	rm -r plugins/$1/dev-tools
	rm -rf plugins/$1/.git
	rm plugins/$1/LICENSE.txt
	rm plugins/$1/CONTRIBUTING.md

	# echo "### commit changes"
	git add --all .
	git commit -m "add $1 module" > /dev/null
	git checkout $GIT_BRANCH
	git merge -m "migrate branch for $1" migrate_$1 > /dev/null 2>/dev/null
	# We can now add plugin module
	insertLinesBefore "    <\/modules>" "        <module>$1<\/module>" "§" "plugins/pom.xml"
	git add .
	git commit -m "add $1 module" > /dev/null
	git branch -D migrate_$1
}

echo "# STEP 0 : prepare the job"


# echo "## create git $GIT_BRANCH work branch"

# It first clean the existing branch if any
echo "(You can safely ignore branch not found below)"
git branch -D $GIT_BRANCH > /dev/null || :

# Create the new branch
git branch $GIT_BRANCH > /dev/null
git checkout $GIT_BRANCH > /dev/null 2>/dev/null


echo "# STEP 4 : Migrate plugins"

# Analysis
migratePlugin "lang-python" 
migratePlugin "lang-javascript"


echo "you can now run:"
echo "mvn clean install -DskipTests"


