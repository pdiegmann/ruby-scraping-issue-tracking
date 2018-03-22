from github import Github
import json
import sys
import datetime
import time
import jsonlines
import os
import shutil

def main():
    data_folder = os.path.join('..', '..', '..', 'data')
    archive_folder = os.path.join('..', '..', '..', 'archive')

    users = {}
    issues = {}

    if os.path.exists(data_folder) and os.listdir(data_folder) != []:
        if raw_input("enter y to continue previous crawl, all other inputs will start a new crawl: ") == "y":
            for directory_obj in os.listdir(os.path.join(data_folder, 'github')):
                if not directory_obj.endswith('.jsonl'): continue
                with jsonlines.open(os.path.join(data_folder, 'github', directory_obj), mode='r') as reader:
                    for obj in reader:
                        if directory_obj.startswith('users-'):
                            users[obj['id']] = obj
                        elif directory_obj.startswith('issues-'):
                            issues[obj['id']] = obj
        else:
            shutil.move(data_folder, os.path.join(archive_folder, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')))

    if not os.path.exists(data_folder):
        os.mkdir(data_folder)

    if not os.path.exists(os.path.join(data_folder, 'github')):
        os.mkdir(os.path.join(data_folder, 'github'))

    print 'starting...'
    g = Github("75306889b3e191168d86d0547577892a2a24f2dd")

    print 'fetching and processing issues...'
    counter = 0
    start_time = time.time()

    with jsonlines.open(os.path.join(data_folder, 'github', 'users-' + time.strftime('%Y%m%d-%H%M%S', time.localtime(start_time))) + '.jsonl', mode='w', flush=True) as user_writer:
        with jsonlines.open(os.path.join(data_folder, 'github', 'issues-' + time.strftime('%Y%m%d-%H%M%S', time.localtime(start_time))) + '.jsonl', mode='w', flush=True) as issue_writer:
            for issue in g.get_organization('ruby').get_repo('ruby').get_issues(state='all', sort='created', direction='asc'):
                if issue.id in issues:
                    print ' >>> SKIPPING ISSUE ' + str(issue.id) + ' <<< ',
                    continue

                labels = []
                for label in issue.labels:
                    labels.append(label.name)

                assignees = []
                if issue.assignee is not None:
                    assignees.append(parse_and_add_user(users, user_writer, issue.assignee)['id'])

                for assignee in issue.assignees:
                    assignees.append(parse_and_add_user(users, user_writer, assignee)['id'])

                reactions = {}
                for reaction in issue.get_reactions():
                    if reaction.content not in reactions: reactions[reaction.content] = []
                    reactions[reaction.content].append(parse_and_add_user(users, user_writer, reaction.user)['id'])

                issues[issue.id] = {
                    'id': issue.id,
                    'url': issue.url,
                    'number': issue.number,
                    'state': issue.state,
                    'user': parse_and_add_user(users, user_writer, issue.user)['id'],
                    'title': issue.title,
                    'text': issue.body,
                    'labels': labels,
                    'assignees': assignees,
                    'is-pull-request': issue.pull_request is not None,
                    'closed_at': str(issue.closed_at),
                    'created_at': str(issue.created_at),
                    'updated_at': str(issue.updated_at),
                    'reactions': reactions,
                    'comments': []
                }

                for comment in issue.get_comments():
                    reactions = {}
                    for reaction in comment.get_reactions():
                        if reaction.content not in reactions: reactions[reaction.content] = []
                        reactions[reaction.content].append(parse_and_add_user(users, user_writer, reaction.user)['id'])

                    issues[issue.id]['comments'].append({
                        'id': comment.id,
                        'text': comment.body,
                        'url': comment.url,
                        'user': parse_and_add_user(users, user_writer, comment.user)['id'],
                        'created_at': str(comment.created_at),
                        'reactions': reactions
                    })

                issue_writer.write(issues[issue.id])

                print '.',
                counter = counter + 1
                if counter % 10 == 0:
                    duration = time.time() - start_time
                    print '| ' + str(counter) + ' (' + str(datetime.timedelta(seconds=duration)) + ') |',
                    sys.stdout.flush()

#        with open('../../../data/github/users.json' ,'w') as output:
#            json.dump(users, output, default=datetime_json_serializer)
#
#        with open('../../../data/github/issues.json' ,'w') as output:
#            json.dump(issues, output, default=datetime_json_serializer)

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
