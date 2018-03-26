import json
import jsonlines
import os
import datetime
import sys
import mmap

def main():
    data_folder = os.path.join('..', '..', '..', 'data')
    archive_folder = os.path.join('..', '..', '..', 'archive')

    users = {}
    issues = {}

    if os.path.exists(data_folder) and os.path.exists(os.path.join(data_folder, 'github')) and os.listdir(os.path.join(data_folder, 'github')) != []:
        read_users_and_issues(data_folder, users, issues)
        target = os.path.join(archive_folder, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
        write_all_to_file(users, os.path.join(target, 'users.jsonl'), 'writing users...')
        write_all_to_file(issues, os.path.join(target, 'issues.jsonl'), 'writing issues...')

        print 'Done!'
    else:
        print 'Nothing to do.'


def write_all_to_file(obj, target, title):
    startProgress(title)

    if not os.path.exists(os.path.dirname(target)):
        os.mkdir(os.path.dirname(target))

    total_count_items = len(obj)
    count_items = 0.0
    with jsonlines.open(target, mode='w') as writer:
        for index, item in obj.iteritems():
            writer.write(item)
            count_items = count_items + 1.0
            progress(count_items / total_count_items * 100)

    endProgress()


def read_users_and_issues(data_folder, users, issues):
    print 'preparing...'
    total_line_count = 0
    line_counter = 0

    for directory_obj in os.listdir(os.path.join(data_folder, 'github')):
        if not directory_obj.endswith('.jsonl'): continue
        total_line_count = total_line_count + file_len(os.path.join(data_folder, 'github', directory_obj))

    startProgress('reading data...')
    for directory_obj in os.listdir(os.path.join(data_folder, 'github')):
        if not directory_obj.endswith('.jsonl'): continue
        with jsonlines.open(os.path.join(data_folder, 'github', directory_obj), mode='r') as reader:
            for obj in reader:
                if directory_obj.startswith('users-'):
                    users[obj['id']] = obj
                elif directory_obj.startswith('issues-'):
                    issues[obj['id']] = obj

                line_counter = line_counter + 1.0
                progress(line_counter / total_line_count * 100)
    endProgress()

def file_len(filename):
    try:
        f = open(filename, "r+")
        buf = mmap.mmap(f.fileno(), 0)
        lines = 0.0
        readline = buf.readline
        while readline():
            lines += 1.0
        return lines
    except Exception as e:
        return 0.0

def startProgress(title):
    global progress_x
    sys.stdout.write(title + ": [" + "-"*40 + "]" + chr(8)*41)
    sys.stdout.flush()
    progress_x = 0

def progress(x):
    global progress_x
    x = int(x * 40 // 100)
    if x != progress_x:
        sys.stdout.write("#" * (x - progress_x))
        sys.stdout.flush()
        progress_x = x

def endProgress():
    sys.stdout.write("#" * (40 - progress_x) + "]\n")
    sys.stdout.flush()

if __name__ == "__main__":
    main()
