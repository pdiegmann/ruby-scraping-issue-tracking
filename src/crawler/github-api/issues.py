from github import Github
import json
import sys
import datetime
import time

def main():
    print 'starting...'
    g = Github("75306889b3e191168d86d0547577892a2a24f2dd")

    users = {}
    issues = {}

    print 'fetching and processing issues...'
    counter = 0
    start_time = time.time()
    for issue in g.get_organization('ruby').get_repo('ruby').get_issues():
        labels = []
        for label in issue.labels:
            labels.append(label.name)

        assignees = []
        if issue.assignee is not None:
            assignees.append(parse_and_add_user(users, issue.assignee)['id'])

        for assignee in issue.assignees:
            assignees.append(parse_and_add_user(users, assignee)['id'])

        reactions = {}
        for reaction in issue.get_reactions():
            if reaction.content not in reactions: reactions[reaction.content] = []
            reactions[reaction.content].append(parse_and_add_user(users, reaction.user)['id'])

        issues[issue.id] = {
            'id': issue.id,
            'url': issue.url,
            'number': issue.number,
            'state': issue.state,
            'user': parse_and_add_user(users, issue.user)['id'],
            'title': issue.title,
            'text': issue.body,
            'labels': labels,
            'assignees': assignees,
            'is-pull-request': issue.pull_request is not None,
            'closed_at': issue.closed_at,
            'created_at': issue.created_at,
            'updated_at': issue.updated_at,
            'reactions': reactions,
            'comments': []
        }

        for comment in issue.get_comments():
            reactions = {}
            for reaction in comment.get_reactions():
                if reaction.content not in reactions: reactions[reaction.content] = []
                reactions[reaction.content].append(parse_and_add_user(users, reaction.user)['id'])

            issues[issue.id]['comments'].append({
                'id': comment.id,
                'text': comment.body,
                'url': comment.url,
                'user': parse_and_add_user(users, comment.user)['id'],
                'created_at': comment.created_at,
                'reactions': reactions
            })
        print '.',
        counter = counter + 1
        if counter % 10 == 0:
            duration = time.time() - start_time
            print '| ' + str(counter) + ' (' + str(datetime.timedelta(seconds=duration)) + ') |',
            sys.stdout.flush()
        if counter % 2 == 0: break

    with open('../../../data/github/users.json' ,'w') as output:
        json.dump(users, output, default=datetime_json_serializer)

    with open('../../../data/github/issues.json' ,'w') as output:
        json.dump(issues, output, default=datetime_json_serializer)

def parse_and_add_user(users, user_raw):
    if user_raw is not None and user_raw.id is not None and user_raw.id not in users:
        user_parsed = parse_user(user_raw)
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
