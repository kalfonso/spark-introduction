from faker import Factory
 
#----------------------------------------------------------------------
def create_names(fake):
    """"""
    for i in range(668):
        print "{0},{1}".format((i + 1), fake.name())
 
if __name__ == "__main__":
    fake = Factory.create()
    create_names(fake)
