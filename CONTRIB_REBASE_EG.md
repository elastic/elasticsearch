#### Example
    
For e.g. if you have forked the project into your git account at https://github.com/${userName}/elasticsearch.git 

Clone the repository into your machine.

	git clone https://github.com/${userName}/elasticsearch.git
	
Change into the root of your cloned repository.

    cd <work_path>/elasticsearch
	
Add the original elasticsearch repo as an upstream repo

	git remote add upstream https://github.com/elastic/elasticsearch.git
	
Sync up with the upstream

	git fetch --all upstream
	
	
Verify that the elasticsearch repos are available as remotes

	git remote -v
	
You should see the following in the output of the previous command.

	upstream	https://github.com/elastic/elasticsearch.git (fetch)
	upstream	https://github.com/elastic/elasticsearch.git (push)
	
Pull in all changes from elasticsearch's remote master into your fork's master

	git rebase upstream/master

Make a new local branch to do your work

	git checkout -b <YOUR_BRANCH_NAME>
	
Perform the code changes and commit

	git add .
	git commit -m <"Describe the change">	
	
Push changes on your branch to your remote repo

	git push --set-upstream origin <YOUR_BRANCH_NAME>
	
Switch back to your master branch

	git checkout master
	
Rebase master with elasticsearch's master to get any new commits since you branched.

	git rebase upstream/master
	
Apply your branche's commits onto your fork's master

	git rebase origin/<YOUR_BRANCH_NAME>
	
	
If your fix had multiple commits you want to "squash" them into one to make it easier to review.

	git rebase -i HEAD~<NO_OF_COMMITS>
	
Replace <NO_OF_COMMITS> with the no of commits you made.
	
This will bring up your default git editor, replace the words "pick" with "squash" next to the commits you want to squash into the commit before it. Update the commit message to represent what was implemented / fixed.


Push the changes to your remote master

	git push origin master
	
Open a pull request from your github web ui, there should be only a single commit in the pull request.
