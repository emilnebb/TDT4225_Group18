from DbConnector import DbConnector
from tabulate import tabulate

import os
import datetime


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table_user(self):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id VARCHAR(30) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % "User")
        self.db_connection.commit()

    def create_table_activity(self):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT NOT NULL PRIMARY KEY,
                   user_id VARCHAR(30),
                   transportation_mode VARCHAR(30),
                   start_date_time DATETIME,
                   end_date_time DATETIME,
                   CONSTRAINT user_id_fk FOREIGN KEY (user_id) 
                   REFERENCES User (id))
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % "Activity")
        self.db_connection.commit()

    def create_table_trackpoint(self):
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   activity_id INT,
                   lat DOUBLE,
                   lon DOUBLE,
                   altitude INT,
                   date_days DOUBLE,
                   date_time DATETIME,
                   CONSTRAINT activity_id FOREIGN KEY (activity_id) 
                   REFERENCES Activity (id))
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % "Trackpoint")
        self.db_connection.commit()

    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def insert_data_user(self, id, has_label):
        query = "INSERT INTO User(id, has_labels)  VALUES (%s, %r)"
        self.cursor.execute(query % (id, has_label))
        self.db_connection.commit()

    def insert_data_Activity(self, id, user_id, transportation_mode, start_date_time, end_date_time):
        query = "INSERT INTO Activity(id, " \
                "user_id, " \
                "transportation_mode, " \
                "start_date_time, " \
                "end_date_time) VALUES (%s, %s, %s, %s, %s)"
        recordTuple = (id, user_id, transportation_mode, start_date_time, end_date_time)
        self.cursor.execute(query, recordTuple)
        self.db_connection.commit()

    def insert_data_trackpoints(self, activity_id, lat, lon, altitude, date_days, date_time):
        query = "INSERT INTO Trackpoint( activity_id, lat, lon, altitude, date_days, date_time)" \
                " VALUES ( %s, %s, %s, %s, %s, %s)"
        recordTuple = (activity_id, lat, lon, altitude, date_days, date_time)
        self.cursor.execute(query, recordTuple)
        self.db_connection.commit()

    def getLabeledUsers(self, path):
        a_file = open(path, "r")

        list_of_lists = []
        for line in a_file:
            stripped_line = line.strip()
            line_list = stripped_line.split()
            list_of_lists.append(line_list)

        a_file.close()
        return list_of_lists


def main():
    program = None
    try:
        program = ExampleProgram()

        # Fetching all labeled users from file
        labeled_list = program.getLabeledUsers("dataset\labeled_ids.txt")

        activityID = 1
        for (root, dirs, files) in os.walk('dataset\Data', topdown='true'):
            print(root)
            print(dirs)

            if (len(root) == 16):  # In the xxx-user folder
                userid = root[-3:]
                has_labels = False
                print(userid)

                for user in labeled_list:
                    if (userid == user[0]):
                        has_labels = True
                        break

                program.insert_data_user(userid, has_labels)
                # _ = program.fetch_data(table_name="User")
                # program.show_tables()

                if (has_labels):
                    labels_path = root + "\\labels.txt"
                    labels_file = open(labels_path, "r")

                    activities = []

                    for line in labels_file:
                        activities.append(line)

                    activities = activities[1:]

                    for activity in activities:
                        start_time = activity[:19]
                        end_time = activity[20:39]
                        transportation_mode = activity[40:]
                        # print(start_time)
                        # print(end_time)
                        # print(transportation_mode)
                        program.insert_data_Activity(activityID, int(user_id), transportation_mode, start_time,
                                                     end_time)
                        activityID += 1

            if (len(root) == 27):  # In the Trajectory folder
                for file in files:
                    user_id = root[13:16]
                    # activity_id = file[:14]
                    # print (activityID)

                    activity_file_path = "dataset\\\Data\\" + userid + "\\Trajectory\\" + file
                    if (sum(1 for line in open(activity_file_path)) < 2506):
                        activity_file = open(activity_file_path, "r")
                        trajectories = []
                        for line in activity_file:
                            trajectories.append(line)
                        trajectories = trajectories[6:]
                        start_time = trajectories[0][-20:-10] + ' ' + trajectories[0][-9:]
                        end_time = trajectories[-1][-20:-10] + ' ' + trajectories[-1][-9:]
                        # print(user_id)
                        # print(activityID)
                        # print(start_time)
                        # print (end_time)
                        program.insert_data_Activity(activityID, int(user_id), "", start_time, end_time)
                        # _ = program.fetch_data(table_name="Activity")

                        for trajectory in trajectories:
                            data = trajectory.split(",")
                            lat = data[0]
                            lon = data[1]
                            altitude = data[3]
                            date_days = data[4]
                            date_time = data[5] + " " + data[6]
                            # print("Trackpoint")
                            # print(lat)
                            # print(lon)
                            # print(altitude)
                            # print(date_days)
                            # print(date_time)

                            program.insert_data_trackpoints(activityID, lat, lon, altitude, date_days, date_time)
                            # _ = program.fetch_data(table_name="Trackpoint")

                        activityID += 1

            print('--------------------------------')

        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
