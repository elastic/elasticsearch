import argparse
import calendar
import json
import re
import requests
import shutil
import subprocess
import tempfile
import time

def throttle(response):
    if 'X-RateLimit-Remaining' in response.headers:
        rate_limit_remaining = int(response.headers['X-RateLimit-Remaining'])
        if rate_limit_remaining < 16:
            rate_limit_reset = int(response.headers['X-RateLimit-Reset'])
            delay = rate_limit_reset - calendar.timegm(time.gmtime())
            print('rate limit remaining: {}, rate limit reset: {}, throttling for {} seconds'.format(rate_limit_remaining, rate_limit_reset, delay))
            if delay >= 0:
                time.sleep(delay)

    return response

def authorization_token(token):
    return {'Authorization':'token {}'.format(token)}

def next_url(response):
    """Return the next URL following the Link header, otherwise None."""
    nu = None
    if 'Link' in response.headers:
        links = response.headers['Link'].split(',')
        for link in links:
            if 'rel="next"' in link:
                nu = link.split(';')[0][1:-1]
                break

    return nu

def get(url, token):
    """Return the response for the specified URL."""
    headers = authorization_token(token)
    return throttle(requests.get(url, headers=headers))

def get_all(url, token):
    """Returns all pages starting at the specified URL."""
    items = []
    while url is not None:
        response = get(url, token)
        json = response.json()
        if json:
            items.extend(response.json())
            url = next_url(response)
        else:
            url = None

    return items

def issue_comments(source_owner, source_repo, issue, token):
    """Return all issue comments."""
    url = 'https://api.github.com/repos/{}/{}/issues/{}/comments'.format(source_owner, source_repo, issue)
    return get_all(url, token)

assignees_cache = {}

def repo_assignees(assignee, owner, repo, token):
    """Returns True if the assignee is valid for the specified owner/repo, otherwise False."""
    if assignee in assignees_cache:
        return assignees_cache[assignee]
    url = 'https://api.github.com/repos/{}/{}/assignees/{}'.format(owner, repo, assignee)
    response = get(url, token)
    assignees_cache[assignee] = response.status_code == 204
    return assignees_cache[assignee]

def rewrite_issue_links(text, source_owner, source_repo):
    return re.sub(r"(\s+)(#\d+)", "\\1{}/{}\\2".format(source_owner, source_repo), text)

def rewrite_commits(text, source_owner, source_repo, temp):
    commits = []
    for match in re.finditer(r"(?<!@)[a-f0-9]{7,40}", text):
        if subprocess.call(['git', '-C', temp, 'cat-file', 'commit', match.group(0)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0:
            commits.append(match.group(0))

    for commit in commits:
        text = re.sub(r"(?<!@)({})".format(commit), "{}/{}@{}".format(source_owner, source_repo, commit), text)

    return text

def copy_issue(issue, label_names, source_owner, source_repo, destination_owner, destination_repo, token, temp):
    """Posts an issue to the specified owner/repo with the specified labels."""
    url = 'https://api.github.com/repos/{}/{}/issues'.format(destination_owner, destination_repo)
    headers = authorization_token(token)
    issue_number = issue['number']
    title = issue['title']
    issue_body = rewrite_commits(rewrite_issue_links(issue['body'], source_owner, source_repo), source_owner, source_repo, temp)
    body = '*Original comment by @{}:*'.format(issue['user']['login']) + '\n\n' + issue_body + '\n\n' + 'Supercedes {}/{}#{}'.format(source_owner, source_repo, issue_number)
    labels = [label for label in label_names]
    assignees = [assignee['login'] for assignee in issue['assignees'] if repo_assignees(assignee['login'], destination_owner, destination_repo, token)]
    payload = { 'title':title, 'body':body, 'labels':labels, 'assignees':assignees }
    return throttle(requests.post(url, headers=headers, data=json.dumps(payload)))

def close_issue(issue_number, owner, repo, token):
    url = 'https://api.github.com/repos/{}/{}/issues/{}'.format(owner, repo, issue_number)
    headers = authorization_token(token)
    payload = { 'state':'closed' }
    return throttle(requests.post(url, headers=headers, data=json.dumps(payload)))

def copy_comment(comment, source_owner, source_repo, destination_owner, destination_repo, issue_number, token, temp):
    """Copies a comment to the specified issue in the owner/repo."""
    comment_body = rewrite_commits(rewrite_issue_links(comment['body'], source_owner, source_repo), source_owner, source_repo, temp)
    body = '*Original comment by @{}:*'.format(comment['user']['login']) + '\n\n' + comment_body
    return post_comment(body, destination_owner, destination_repo, issue_number, token)

def post_comment(body, destination_owner, destination_repo, issue_number, token):
    """Posts a comment to the specified issue in the owner/repo."""
    url = 'https://api.github.com/repos/{}/{}/issues/{}/comments'.format(destination_owner, destination_repo, issue_number)
    headers = authorization_token(token)
    payload = { 'body':body }
    return throttle(requests.post(url, headers=headers, data=json.dumps(payload)))

def post_label(name, color, destination_owner, destination_repo, token):
    url = 'https://api.github.com/repos/{}/{}/labels/{}'.format(destination_owner, destination_repo, name)
    headers = authorization_token(token)
    payload = { 'name':name, 'color':color }
    response = throttle(requests.post(url, headers=headers, data=json.dumps(payload)))
    if response.status_code != 200:
        raise('updating label {} failed'.format(name))

def merge_labels(existing_labels, new_labels):
    existing_labels.update({label['name']:label['color'] for label in new_labels})

def update_labels(labels, destination_owner, destination_repo, token):
    for name, color in labels.items():
        post_label(name, color, destination_owner, destination_repo, token)

def main(source_owner, source_repo, destination_owner, kibana_destination_repo, logstash_destination_repo, elasticsearch_destination_repo, token):
    temp = tempfile.mkdtemp()
    subprocess.call(['git', 'clone', 'https://{}@github.com/{}/{}'.format(token, source_owner, source_repo), temp], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        kibana_labels = {}
        logstash_labels = {}
        elasticsearch_labels = {}
        url = 'https://api.github.com/repos/{}/{}/issues?direction=asc'.format(source_owner, source_repo)
        while url is not None:
            issues = get(url, token)
            for issue in [i for i in issues.json() if 'pull_request' not in i]:
                # issue_number represents the issue number in the source repo
                issue_number = issue['number']
                print('processing issue: {}'.format(issue_number))
                labels = issue['labels']
                label_names = {label['name'] for label in labels}
                if ':UI' in label_names or ':reporting' in label_names:
                    merge_labels(kibana_labels, labels)
                    destination_repo = kibana_destination_repo
                elif ':logstash' in label_names:
                    merge_labels(logstash_labels, labels)
                    destination_repo = logstash_destination_repo
                else:
                    merge_labels(elasticsearch_labels, labels)
                    destination_repo = elasticsearch_destination_repo
                new_issue = copy_issue(issue, label_names, source_owner, source_repo, destination_owner, destination_repo, token, temp)
                # new_issue_number represents the issue number in the destination repo√
                new_issue_number = new_issue.json()['number']
                print('issue {} copied to new issue {} in {}/{}'.format(issue_number, new_issue_number, destination_owner, destination_repo))
                # post each comment from the source issue to the destination issue
                comments = issue_comments(source_owner, source_repo, issue_number, token)
                for comment in comments:
                    copy_comment(comment, source_owner, source_repo, destination_owner, destination_repo, new_issue_number, token, temp)

                # comment on the original issue referring to the new issue
                post_comment('Superceded by {}/{}#{}'.format(destination_owner, destination_repo, new_issue_number), source_owner, source_repo, issue_number, token)
                close_issue(issue_number, source_owner, source_repo, token)
                print('issue {} closed'.format(issue_number))

            # proceed to the next page of issues
            url = next_url(issues)

        # update labels
        update_labels(kibana_labels, destination_owner, kibana_destination_repo, token)
        update_labels(logstash_labels, destination_owner, logstash_destination_repo, token)
        update_labels(elasticsearch_labels, destination_owner, elasticsearch_destination_repo, token)

    finally:
        shutil.rmtree(temp)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Migrate issues')
    parser.add_argument('--source_owner', required=True)
    parser.add_argument('--source_repo', required=True)
    parser.add_argument('--destination_owner', required=True)
    parser.add_argument('--kibana_destination_repo', required=True)
    parser.add_argument('--logstash_destination_repo', required=True)
    parser.add_argument('--elasticsearch_destination_repo', required=True)
    parser.add_argument('--token', required=True)
    args = parser.parse_args()
    main(args.source_owner, args.source_repo, args.destination_owner, args.kibana_destination_repo, args.logstash_destination_repo, args.elasticsearch_destination_repo, args.token)
