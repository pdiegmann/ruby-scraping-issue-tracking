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

def main():
    data_folder = os.path.join('..', '..', '..', 'data')
    archive_folder = os.path.join('..', '..', '..', 'archive')

    users = {}
    issues = {}

    retry = raw_input('enter y to auto-retry on exception, anything else to break: ') == 'y'

    if os.path.exists(data_folder) and os.path.exists(os.path.join(data_folder, 'github')) and os.listdir(os.path.join(data_folder, 'github')) != []:
        if raw_input('enter y to continue previous crawl, all other inputs will start a new crawl: ') == 'y':
            read_users_and_issues(data_folder, users, issues)
        else:
            shutil.move(data_folder, os.path.join(archive_folder, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')))

    if not os.path.exists(data_folder):
        os.mkdir(data_folder)

    if not os.path.exists(os.path.join(data_folder, 'github')):
        os.mkdir(os.path.join(data_folder, 'github'))

    print 'starting...'
    g = Github(login_or_token="75306889b3e191168d86d0547577892a2a24f2dd", timeout=60, per_page=100, user_agent='UniversityOfCologneSNACrawl/0.1')

    print 'fetching and processing issues...'
    counter = 0
    start_time = time.time()

    with jsonlines.open(os.path.join(data_folder, 'github', 'users-' + time.strftime('%Y%m%d-%H%M%S', time.localtime(start_time))) + '.jsonl', mode='w', flush=True) as user_writer:
        with jsonlines.open(os.path.join(data_folder, 'github', 'issues-' + time.strftime('%Y%m%d-%H%M%S', time.localtime(start_time))) + '.jsonl', mode='w', flush=True) as issue_writer:
            while True:
                try:
                    since = get_oldest_issue_date_or_not_set(issues)
                    print 'selecting issues since: ' + str(since)
                    print 'already crawled ' + str(len(issues)) + ' issues and ' + str(len(users)) + ' users'

                    handle_rate_limit(g)
                    fetch_and_parse_issues(g, user_writer, issue_writer, users, issues, since, counter, start_time)
                    break
                except Exception as e:
                    print e
                    if retry:
                        print '\n\n>>> STARTING RETRY <<<\n\n'
                    else:
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

def get_oldest_issue_date_or_not_set(issues):
    since = None
    for key, issue in issues.iteritems():
        issue_created_at = parser.parse(issue['created_at'])
        if since is None or since < issue_created_at:
            since = issue_created_at

    if since is None: since = GithubObject.NotSet
    return since

def read_users_and_issues(data_folder, users, issues):
    for directory_obj in os.listdir(os.path.join(data_folder, 'github')):
        if not directory_obj.endswith('.jsonl'): continue
        with jsonlines.open(os.path.join(data_folder, 'github', directory_obj), mode='r') as reader:
            for obj in reader:
                if directory_obj.startswith('users-'):
                    users[obj['id']] = obj
                elif directory_obj.startswith('issues-'):
                    issues[obj['id']] = obj

def fetch_and_parse_issues(g, user_writer, issue_writer, users, issues, since, counter, start_time):
    skip_counter = 0
    # since only looks for updates. But we sort by creation_date, so we cannot really skip all found items...
    for issue in g.get_organization('rails').get_repo('rails').get_issues(state='all', since=since, sort='created', direction='asc'):
        if issue.id in issues:
            skip_counter = skip_counter + 1
            print ' >>> SKIPPING ' + str(issue.id) + ' (' + str(issue.created_at) + ') #' + str(skip_counter) + ' <<< ',
            continue

        labels = []
        for label in issue.labels:
            labels.append(label.name)

        assignees = []
        if issue.assignee is not None:
            assignees.append(parse_and_add_user(users, user_writer, issue.assignee)['id'])

        for assignee in issue.assignees:
            assignees.append(parse_and_add_user(users, user_writer, assignee)['id'])

        reactions = parse_reactions(issue, users, user_writer)

        issues[issue.id] = convert_issue(issue, parse_and_add_user(users, user_writer, issue.user)['id'], labels, assignees, reactions)

        for comment in issue.get_comments():
            reactions = parse_reactions(issue, users, user_writer)
            user_id = parse_and_add_user(users, user_writer, comment.user)['id']
            issues[issue.id]['comments'].append(convert_comment(comment, user_id, reactions))

        issue_writer.write(issues[issue.id])

        print '.',
        counter = counter + 1
        if counter % 10 == 0:
            duration = time.time() - start_time
            print '| ' + str(counter) + ' (' + str(datetime.timedelta(seconds=duration)) + ') |',
        sys.stdout.flush()

        handle_rate_limit(g)

def convert_comment(comment, user_id, reactions):
    return {
        'id': comment.id,
        'text': comment.body,
        'url': comment.url,
        'user': user_id,
        'created_at': str(comment.created_at),
        'reactions': reactions
    }

def parse_reactions(issue_or_comment, users, user_writer):
    reactions = {}
    for reaction in issue_or_comment.get_reactions():
        if reaction.content not in reactions: reactions[reaction.content] = []
        reactions[reaction.content].append(parse_and_add_user(users, user_writer, reaction.user)['id'])

    return reactions

def convert_issue(issue, user_id, labels, assignee_ids, reactions):
    return {
        'id': issue.id,
        'url': issue.url,
        'number': issue.number,
        'state': issue.state,
        'user': user_id,
        'title': issue.title,
        'text': issue.body,
        'labels': labels,
        'assignees': assignee_ids,
        'is-pull-request': issue.pull_request is not None,
        'closed_at': str(issue.closed_at),
        'created_at': str(issue.created_at),
        'updated_at': str(issue.updated_at),
        'reactions': reactions,
        'comments': []
    }

def parse_and_add_user(users, writer, user_raw):
    if user_raw is not None and user_raw.id is not None and user_raw.id not in users:
        user_parsed = parse_user(user_raw)
        if writer is not None:
            writer.write(user_parsed)
        users[user_raw.id] = user_parsed
        return user_parsed
    elif user_raw.id in users:
        return users[user_raw.id]
    else:
        print "not found: " + str(user_raw.id)

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
