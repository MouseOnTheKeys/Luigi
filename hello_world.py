import luigi
import os

from time import sleep


class HelloTask(luigi.Task):
    path = luigi.Parameter()

    def run(self):
        sleep(60)  # 60 seconds delay
        with open("hello.txt", "w") as hello_file:
            hello_file.write("Hello")
            hello_file.close()

    def output(self):
        return luigi.LocalTarget(self.path)
        # return luigi.LocalTarget("hello.txt")

    def requires(self):
        return [
            MakeDirectory(path=os.path.dirname(self.path)),
        ]


class WorldTask(luigi.Task):
    path = luigi.Parameter()

    def run(self):
        sleep(30)
        with open("world.txt", "w") as world_file:
            world_file.write("World")
            world_file.close()

    def output(self):
        return luigi.LocalTarget(self.path)
        # return luigi.LocalTarget("world.txt")

    def requires(self):
        return [
            MakeDirectory(path=os.path.dirname(self.path)),
        ]


# class HelloWorldTask(luigi.Task):
#     def run(self):
#         with open("hello.txt", "r") as hello_file:
#             hello = hello_file.read()
#         with open("world.txt", "r") as world_file:
#             world = world_file.read()
#         with open("hello_world.txt", "w") as output_file:
#             content = "{} {} !".format(hello, world)
#             output_file.write(content)
#             output_file.close()

#     def requires(self):
#         return [HelloTask(), WorldTask()]

#     def output(self):
#         return luigi.LocalTarget("hello_world.txt")


class HelloWorldTask(luigi.Task):
    id = luigi.Parameter(default="test")

    def output(self):
        path = "results/{}/hello_world.txt".format(self.id)
        return luigi.LocalTarget(path)

    def requires(self):
        return [
            PrintWordTask(path="results/{}/hello.txt".format(self.id), word="Hello"),
            PrintWordTask(path="results/{}/world.txt".format(self.id), word="World"),
        ]

    def run(self):
        with open(self.input()[0].path, "r") as hello_file:
            hello = hello_file.read()
        with open(self.input()[1].path, "r") as world_file:
            world = world_file.read()
        with open(self.output().path, "w") as output_file:
            content = "{} {} !".format(hello, world)
            output_file.write(content)
            output_file.close()


class MakeDirectory(luigi.Task):
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        os.makedirs(self.path)


class PrintWordTask(luigi.Task):
    path = luigi.Parameter()
    word = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        with open(self.path, "w") as out_file:
            out_file.write(self.word)
            out_file.close()


if __name__ == "__main__":
    luigi.run()
