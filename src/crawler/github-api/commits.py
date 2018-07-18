from github import Github
from github import GithubObject
import json
import sys
import datetime
from datetime import time
from time import sleep
import time
import jsonlines
import os
import shutil
from dateutil import parser
import json
from tqdm import tqdm

def main():
    credentials = { 'github_token': None }
    with open('../../../credentials.json') as f:
        credentials = json.load(f)

    data_folder = os.path.join('..', '..', '..', 'data')
    archive_folder = os.path.join('..', '..', '..', 'archive')

    users = {}
    commits = {}

    retry = raw_input('enter y to auto-retry on exception, anything else to break: ') == 'y'

    if os.path.exists(data_folder) and os.path.exists(os.path.join(data_folder, 'github')) and os.listdir(os.path.join(data_folder, 'github')) != []:
        if raw_input('enter y to continue previous crawl, all other inputs will start a new crawl: ') == 'y':
            read_users_and_commits(data_folder, users, commits)
        else:
            shutil.move(data_folder, os.path.join(archive_folder, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')))

    if not os.path.exists(data_folder):
        os.mkdir(data_folder)

    if not os.path.exists(os.path.join(data_folder, 'github-commits')):
        os.mkdir(os.path.join(data_folder, 'github-commits'))

    print 'starting...'
    # old token is invalid by no_top_words
    # to generate a personal token, see: https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/
    # HOWEVER, it should not be commited publicly.
    g = Github(login_or_token=credentials['github_token'], timeout=60, per_page=100, user_agent='UniversityOfCologneSNACrawl/0.1')

    print 'fetching and processing commits...'
    counter = 0
    start_time = time.time()

    with jsonlines.open(os.path.join(data_folder, 'github-commits', 'users-' + time.strftime('%Y%m%d-%H%M%S', time.localtime(start_time))) + '.jsonl', mode='w', flush=True) as user_writer:
        with jsonlines.open(os.path.join(data_folder, 'github-commits', 'commits-' + time.strftime('%Y%m%d-%H%M%S', time.localtime(start_time))) + '.jsonl', mode='w', flush=True) as commit_writer:
            while True:
                try:
                    since = get_oldest_commit_date_or_not_set(commits)
                    print 'selecting commits since: ' + str(since)
                    print 'already crawled ' + str(len(commits)) + ' commits and ' + str(len(users)) + ' users'

                    handle_rate_limit(g)
                    fetch_and_parse_commits(g, user_writer, commit_writer, users, commits, since, counter, start_time)
                    break
                except Exception as e:
                    if retry:
                        print e, e.args
                        print '\n\n>>> STARTING RETRY <<<\n\n'
                    else:
                        exc_info = sys.exc_info()
                        raise exc_info[0], exc_info[1], exc_info[2]
                        break

def handle_rate_limit(g):
    remaining, limit = g.rate_limiting
    if remaining <= 10:
        reset_time = datetime.datetime.fromtimestamp(g.rate_limiting_resettime)
        print '\n\nsleeping from ' + str(datetime.datetime.today()) + ' until ' + str(reset_time),
        sys.stdout.flush()

        while reset_time.time() > datetime.datetime.today().time():
            sleep(10)
            print '.',
            sys.stdout.flush()

        print 'continue!\n\n'
        sys.stdout.flush()

def get_oldest_commit_date_or_not_set(commits):
    since = None
    for key, commit in commits.iteritems():
        commit_created_at = parser.parse(commit['created-at'])
        if since is None or since < commit_created_at:
            since = commit_created_at

    if since is None: since = GithubObject.NotSet
    return since

def read_users_and_commits(data_folder, users, commits):
    for directory_obj in os.listdir(os.path.join(data_folder, 'github')):
        if not directory_obj.endswith('.jsonl'): continue
        with jsonlines.open(os.path.join(data_folder, 'github', directory_obj), mode='r') as reader:
            for obj in reader:
                if directory_obj.startswith('users-'):
                    users[obj['id']] = obj
                elif directory_obj.startswith('commits-'):
                    commits[obj['id']] = obj

def fetch_and_parse_commits(g, user_writer, commit_writer, users, commits, since, counter, start_time):
    skip_counter = 0
    for commit in tqdm(g.get_organization('rails').get_repo('rails').get_commits(until=since)):
        if commit.sha in commits:
            skip_counter = skip_counter + 1
            #print ' >>> SKIPPING ' + str(commit.sha) + ' (' + str(commit.commit.committer.date) + ') #' + str(skip_counter) + ' <<< ',
            continue

        author = parse_and_add_user(users, user_writer, commit.author)
        committer = parse_and_add_user(users, user_writer, commit.committer)
        commits[commit.sha] = convert_commit(commit, committer['id'], author['id'])

        commit_writer.write(commits[commit.sha])

        #print '.',
        #counter = counter + 1
        #if counter % 10 == 0:
            #duration = time.time() - start_time
            #print '| ' + str(counter) + ' (' + str(datetime.timedelta(seconds=duration)) + ') |',
        sys.stdout.flush()

        handle_rate_limit(g)

def convert_commit(commit, committer_id, author_id):
    parents = []
    if commit.parents:
        parents = [parent.sha for parent in commit.parents]

    return {
        'id': commit.sha,
        'url': commit.url,
        'parents': parents,
        'text': commit.commit.message,
        'committer': committer_id,
        'author': author_id,
        'tree': commit.commit.tree.sha if commit.commit.tree else None,
        'created-at': str(commit.commit.committer.date),
        'created-at-author': str(commit.commit.author.date),
        'comments-url': commit.comments_url
    }

def parse_and_add_user(users, writer, user_raw):
    if user_raw is not None and user_raw.id is not None and user_raw.id not in users:
        user_parsed = parse_user(user_raw)
        if writer is not None:
            writer.write(user_parsed)
        users[user_raw.id] = user_parsed
        return user_parsed
    elif user_raw is not None and user_raw.id in users:
        return users[user_raw.id]
    else:
        return { 'id': None }

def parse_user(user):
    return {
        'id': user.id,
        'name': user.login,
        'type': user.type,
        'is_admin': user.site_admin
    }

def datetime_json_serializer(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

if __name__ == "__main__":
    main()
